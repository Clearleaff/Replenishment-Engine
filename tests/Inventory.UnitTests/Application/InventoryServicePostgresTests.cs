using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using eShop.EventBus.Abstractions;
using eShop.EventBus.Events;
using eShop.IntegrationEventLogEF;
using eShop.IntegrationEventLogEF.Services;
using eShop.Inventory.API.IntegrationEvents;
using eShop.Inventory.API.IntegrationEvents.Events;
using eShop.Inventory.API;

namespace eShop.Inventory.UnitTests.Application;

[TestClass]
[DoNotParallelize]
public sealed class InventoryServicePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("INVENTORY_TEST_CONNECTION");

    [TestMethod]
    public async Task Reserve_is_atomic_idempotent_location_isolated_and_concurrency_safe()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Assert.Inconclusive("Set INVENTORY_TEST_CONNECTION to run PostgreSQL integration tests.");
        }

        await ResetDatabaseAsync();
        await SeedAsync();

        await using (var context = CreateContext())
        {
            var service = CreateService(context);
            var source = Guid.NewGuid();
            var request = new ReserveInventoryRequest(source, 101, " ncr ",
            [
                new(1, 2),
                new(2, 99)
            ]);

            var rejected = await service.ReserveAsync(request);
            Assert.AreEqual(InventoryOperationOutcome.InsufficientStock, rejected.Outcome);
            Assert.AreEqual(0, (await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR")).Reserved);

            var successRequest = request with { Items = [new(1, 2), new(2, 3)] };
            var success = await service.ReserveAsync(successRequest);
            Assert.AreEqual(InventoryOperationOutcome.Succeeded, success.Outcome);

            var duplicate = await service.ReserveAsync(successRequest);
            Assert.AreEqual(InventoryOperationOutcome.AlreadyProcessed, duplicate.Outcome);
            var sameOrderWithNewMessageId = await service.ReserveAsync(successRequest with { OperationId = Guid.NewGuid() });
            Assert.AreEqual(InventoryOperationOutcome.AlreadyProcessed, sameOrderWithNewMessageId.Outcome);
            var conflictingRedelivery = await service.ReserveAsync(successRequest with
            {
                OperationId = Guid.NewGuid(),
                Items = [new(1, 3), new(2, 3)]
            });
            Assert.AreEqual(InventoryOperationOutcome.Conflict, conflictingRedelivery.Outcome);
            context.ChangeTracker.Clear();
            Assert.AreEqual(2, (await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR")).Reserved);
            Assert.AreEqual(0, (await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "BLR")).Reserved);
        }

        var first = ReserveConcurrentlyAsync(Guid.NewGuid(), 201, 7);
        var second = ReserveConcurrentlyAsync(Guid.NewGuid(), 202, 7);
        var results = await Task.WhenAll(first, second);

        Assert.AreEqual(1, results.Count(result => result.Outcome == InventoryOperationOutcome.Succeeded));
        Assert.AreEqual(1, results.Count(result => result.Outcome == InventoryOperationOutcome.InsufficientStock));
        await using var verification = CreateContext();
        Assert.AreEqual(9, (await verification.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR")).Reserved);
    }

    [TestMethod]
    public async Task Commit_release_and_restock_preserve_inventory_math_and_idempotency()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Assert.Inconclusive("Set INVENTORY_TEST_CONNECTION to run PostgreSQL integration tests.");
        }

        await ResetDatabaseAsync();
        await SeedAsync();
        await using var context = CreateContext();
        var service = CreateService(context);

        await service.ReserveAsync(new(Guid.NewGuid(), 301, "NCR", [new(1, 4)]));
        var commitId = Guid.NewGuid();
        Assert.AreEqual(InventoryOperationOutcome.Succeeded, (await service.CommitAsync(new(commitId, 301, "NCR"))).Outcome);
        Assert.AreEqual(InventoryOperationOutcome.AlreadyProcessed, (await service.CommitAsync(new(commitId, 301, "NCR"))).Outcome);

        await service.ReserveAsync(new(Guid.NewGuid(), 302, "NCR", [new(2, 4)]));
        Assert.AreEqual(InventoryOperationOutcome.Succeeded, (await service.ReleaseAsync(new(Guid.NewGuid(), 302, "NCR"))).Outcome);

        var restock = await service.RestockAsync(new(Guid.NewGuid(), 1, "NCR", 100, "delivery"));
        Assert.AreEqual(InventoryOperationOutcome.Succeeded, restock.Outcome);

        context.ChangeTracker.Clear();
        var sold = await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR");
        var released = await context.InventoryBalances.SingleAsync(b => b.SkuId == 2 && b.LocationCode == "NCR");
        Assert.AreEqual(sold.MaxStock, sold.OnHand);
        Assert.AreEqual(0, sold.Reserved);
        Assert.AreEqual(10, released.OnHand);
        Assert.AreEqual(0, released.Reserved);
        Assert.AreEqual(5, await context.InventoryMovements.CountAsync());
    }

    [TestMethod]
    public async Task Event_processor_persists_business_change_inbox_outbox_and_ignores_duplicate_delivery()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Assert.Inconclusive("Set INVENTORY_TEST_CONNECTION to run PostgreSQL integration tests.");
        }

        await ResetDatabaseAsync();
        await SeedAsync();
        await using var context = CreateContext();
        var bus = new RecordingEventBus();
        var eventLog = new IntegrationEventLogService<InventoryContext>(context);
        var processor = new InventoryIntegrationEventProcessor(
            context,
            CreateService(context),
            new InventoryIntegrationEventService(NullLogger<InventoryIntegrationEventService>.Instance, bus, eventLog),
            Options.Create(new InventoryOptions { ShadowMode = false }),
            NullLogger<InventoryIntegrationEventProcessor>.Instance);
        var incoming = new OrderStatusChangedToAwaitingValidationIntegrationEvent(401, "NCR", [new(1, 3)]);

        await processor.ProcessAsync(incoming);
        await processor.ProcessAsync(incoming);

        context.ChangeTracker.Clear();
        Assert.AreEqual(3, (await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR")).Reserved);
        Assert.AreEqual(1, await context.IncomingIntegrationEvents.CountAsync());
        Assert.AreEqual(1, await context.InventoryMovements.CountAsync());
        Assert.AreEqual(1, await context.Set<IntegrationEventLogEntry>().CountAsync());
        Assert.HasCount(1, bus.Published);
        Assert.IsInstanceOfType<OrderStockConfirmedIntegrationEvent>(bus.Published.Single());
    }

    [TestMethod]
    public async Task Shadow_mode_records_comparison_without_reserving_or_publishing()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Assert.Inconclusive("Set INVENTORY_TEST_CONNECTION to run PostgreSQL integration tests.");
        }

        await ResetDatabaseAsync();
        await SeedAsync();
        await using var context = CreateContext();
        var bus = new RecordingEventBus();
        var processor = new InventoryIntegrationEventProcessor(
            context,
            CreateService(context),
            new InventoryIntegrationEventService(
                NullLogger<InventoryIntegrationEventService>.Instance,
                bus,
                new IntegrationEventLogService<InventoryContext>(context)),
            Options.Create(new InventoryOptions { ShadowMode = true }),
            NullLogger<InventoryIntegrationEventProcessor>.Instance);

        await processor.ProcessAsync(new OrderStatusChangedToAwaitingValidationIntegrationEvent(501, "NCR", [new(1, 2)]));
        await processor.ObserveCatalogAsync(new OrderStockConfirmedIntegrationEvent(501), 501, confirmed: true);

        context.ChangeTracker.Clear();
        var check = await context.InventoryShadowChecks.SingleAsync();
        Assert.IsTrue(check.InventoryConfirmed);
        Assert.IsTrue(check.CatalogConfirmed);
        Assert.IsTrue(check.Agreement);
        Assert.AreEqual(0, (await context.InventoryBalances.SingleAsync(b => b.SkuId == 1 && b.LocationCode == "NCR")).Reserved);
        Assert.IsEmpty(bus.Published);
    }

    [TestMethod]
    public async Task Authoritative_event_lifecycles_commit_sales_release_cancellations_and_ignore_duplicates()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Assert.Inconclusive("Set INVENTORY_TEST_CONNECTION to run PostgreSQL integration tests.");
        }

        await ResetDatabaseAsync();
        await SeedAsync();
        await using var context = CreateContext();
        var bus = new RecordingEventBus();
        var processor = new InventoryIntegrationEventProcessor(
            context,
            CreateService(context),
            new InventoryIntegrationEventService(
                NullLogger<InventoryIntegrationEventService>.Instance,
                bus,
                new IntegrationEventLogService<InventoryContext>(context)),
            Options.Create(new InventoryOptions { ShadowMode = false }),
            NullLogger<InventoryIntegrationEventProcessor>.Instance);

        await processor.ProcessAsync(new OrderStatusChangedToAwaitingValidationIntegrationEvent(601, "NCR", [new(1, 4)]));
        var paid = new OrderStatusChangedToPaidIntegrationEvent(601, "NCR");
        await processor.ProcessAsync(paid);
        await processor.ProcessAsync(paid);

        await processor.ProcessAsync(new OrderStatusChangedToAwaitingValidationIntegrationEvent(602, "NCR", [new(2, 4)]));
        var cancelled = new OrderStatusChangedToCancelledIntegrationEvent(602, "NCR");
        await processor.ProcessAsync(cancelled);
        await processor.ProcessAsync(cancelled);

        context.ChangeTracker.Clear();
        var sold = await context.InventoryBalances.SingleAsync(balance => balance.SkuId == 1 && balance.LocationCode == "NCR");
        var released = await context.InventoryBalances.SingleAsync(balance => balance.SkuId == 2 && balance.LocationCode == "NCR");
        Assert.AreEqual(6, sold.OnHand);
        Assert.AreEqual(0, sold.Reserved);
        Assert.AreEqual(10, released.OnHand);
        Assert.AreEqual(0, released.Reserved);
        Assert.AreEqual(2, await context.InventoryMovements.CountAsync(movement => movement.MovementType == InventoryMovementType.Reserve));
        Assert.AreEqual(1, await context.InventoryMovements.CountAsync(movement => movement.MovementType == InventoryMovementType.Sale));
        Assert.AreEqual(1, await context.InventoryMovements.CountAsync(movement => movement.MovementType == InventoryMovementType.Release));
        Assert.HasCount(2, bus.Published);
    }

    private static async Task<InventoryOperationResult> ReserveConcurrentlyAsync(Guid operationId, int orderId, int quantity)
    {
        await using var context = CreateContext();
        return await CreateService(context).ReserveAsync(new(operationId, orderId, "NCR", [new(1, quantity)]));
    }

    private static InventoryService CreateService(InventoryContext context) =>
        new(context, NullLogger<InventoryService>.Instance);

    private static InventoryContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<InventoryContext>().UseNpgsql(ConnectionString).Options;
        return new InventoryContext(options);
    }

    private static async Task ResetDatabaseAsync()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.MigrateAsync();
    }

    private static async Task SeedAsync()
    {
        await using var context = CreateContext();
        context.Locations.AddRange(new InventoryLocation("NCR", "National Capital Region"), new InventoryLocation("BLR", "Bengaluru"));
        context.InventoryBalances.AddRange(
            new InventoryBalance(1, "NCR", 10, 0, 2, 5, 20),
            new InventoryBalance(2, "NCR", 10, 0, 2, 5, 20),
            new InventoryBalance(1, "BLR", 10, 0, 2, 5, 20),
            new InventoryBalance(2, "BLR", 10, 0, 2, 5, 20));
        await context.SaveChangesAsync();
    }

    private sealed class RecordingEventBus : IEventBus
    {
        public List<IntegrationEvent> Published { get; } = [];
        public Task PublishAsync(IntegrationEvent integrationEvent)
        {
            Published.Add(integrationEvent);
            return Task.CompletedTask;
        }
    }
}
