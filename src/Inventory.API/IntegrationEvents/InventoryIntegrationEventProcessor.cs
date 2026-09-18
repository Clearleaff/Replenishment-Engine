using System.Data;
using eShop.Inventory.API.IntegrationEvents.Events;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;

namespace eShop.Inventory.API.IntegrationEvents;

public sealed class InventoryIntegrationEventProcessor(
    InventoryContext context,
    IInventoryService inventory,
    InventoryIntegrationEventService publisher,
    IOptions<InventoryOptions> options,
    ILogger<InventoryIntegrationEventProcessor> logger)
{
    public async Task ProcessAsync(OrderStatusChangedToAwaitingValidationIntegrationEvent integrationEvent)
    {
        if (options.Value.ShadowMode)
        {
            await ProcessOnceAsync(integrationEvent, async () =>
            {
                var unavailable = new List<int>();
                foreach (var item in integrationEvent.OrderStockItems)
                {
                    var balance = await inventory.GetAsync(item.ProductId, integrationEvent.LocationCode, default);
                    if (balance is null || balance.Available < item.Units)
                    {
                        unavailable.Add(item.ProductId);
                    }
                }

                var confirmed = unavailable.Count == 0;
                context.InventoryShadowChecks.Add(new InventoryShadowCheck(
                    integrationEvent.Id,
                    integrationEvent.OrderId,
                    integrationEvent.LocationCode,
                    confirmed,
                    confirmed ? "Inventory would confirm every line." : $"Inventory would reject SKUs: {string.Join(',', unavailable)}.",
                    DateTime.UtcNow));
                logger.LogInformation("Shadow inventory result for order {OrderId}: {Confirmed}", integrationEvent.OrderId, confirmed);
                return (new InventoryOperationResult(InventoryOperationOutcome.Succeeded, "Shadow evaluation recorded."), null);
            });
            return;
        }

        await ProcessOnceAsync(integrationEvent, async () =>
        {
            var result = await inventory.ReserveAsync(new ReserveInventoryRequest(
                integrationEvent.Id,
                integrationEvent.OrderId,
                integrationEvent.LocationCode,
                integrationEvent.OrderStockItems.Select(item => new InventoryItemRequest(item.ProductId, item.Units)).ToArray()));
            IntegrationEvent outgoing = result.IsSuccess
                ? new OrderStockConfirmedIntegrationEvent(integrationEvent.OrderId)
                : new OrderStockRejectedIntegrationEvent(
                    integrationEvent.OrderId,
                    integrationEvent.OrderStockItems
                        .Select(item => new ConfirmedOrderStockItem(item.ProductId, result.UnavailableSkuIds?.Contains(item.ProductId) != true))
                        .ToList());
            return (result, outgoing);
        });
    }

    public Task ProcessAsync(OrderStatusChangedToPaidIntegrationEvent integrationEvent) =>
        ProcessOnceAsync(integrationEvent, async () =>
        {
            if (options.Value.ShadowMode)
            {
                return (new InventoryOperationResult(InventoryOperationOutcome.AlreadyProcessed, "Shadow mode does not commit stock."), null);
            }

            var result = await inventory.CommitAsync(new(integrationEvent.Id, integrationEvent.OrderId, integrationEvent.LocationCode));
            return (result, null);
        });

    public Task ProcessAsync(OrderStatusChangedToCancelledIntegrationEvent integrationEvent) =>
        ProcessOnceAsync(integrationEvent, async () =>
        {
            if (options.Value.ShadowMode)
            {
                return (new InventoryOperationResult(InventoryOperationOutcome.AlreadyProcessed, "Shadow mode does not release stock."), null);
            }

            var result = await inventory.ReleaseAsync(new(integrationEvent.Id, integrationEvent.OrderId, integrationEvent.LocationCode));
            return (result, null);
        });

    public async Task ObserveCatalogAsync(IntegrationEvent integrationEvent, int orderId, bool confirmed)
    {
        if (!options.Value.ShadowMode)
        {
            return;
        }

        await ProcessOnceAsync(integrationEvent, async () =>
        {
            var check = await context.InventoryShadowChecks
                .Where(candidate => candidate.OrderId == orderId)
                .OrderByDescending(candidate => candidate.EvaluatedAt)
                .FirstOrDefaultAsync();
            check?.ObserveCatalogResult(confirmed, DateTime.UtcNow);
            if (check is not null)
            {
                logger.LogInformation(
                    "Shadow comparison for order {OrderId}: Catalog={Catalog}, Inventory={Inventory}, Agreement={Agreement}",
                    orderId,
                    confirmed,
                    check.InventoryConfirmed,
                    check.Agreement);
            }

            return (new InventoryOperationResult(InventoryOperationOutcome.Succeeded, "Catalog result observed."), null);
        });
    }

    private async Task ProcessOnceAsync(
        IntegrationEvent incoming,
        Func<Task<(InventoryOperationResult Result, IntegrationEvent? Outgoing)>> operation)
    {
        var strategy = context.Database.CreateExecutionStrategy();
        var outgoing = await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync(IsolationLevel.Serializable);
            if (await context.IncomingIntegrationEvents.AnyAsync(message => message.EventId == incoming.Id))
            {
                await transaction.RollbackAsync();
                logger.LogInformation("Ignoring duplicate incoming integration event {EventId}", incoming.Id);
                return null;
            }

            var (_, eventToPublish) = await operation();
            context.IncomingIntegrationEvents.Add(new IncomingIntegrationEvent(incoming.Id, incoming.GetType().Name, DateTime.UtcNow));
            if (eventToPublish is not null)
            {
                context.Set<IntegrationEventLogEntry>().Add(new IntegrationEventLogEntry(eventToPublish, transaction.TransactionId));
            }

            await context.SaveChangesAsync();
            await transaction.CommitAsync();
            return eventToPublish;
        });

        if (outgoing is not null)
        {
            await publisher.PublishAsync(outgoing);
        }
    }
}
