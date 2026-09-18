namespace eShop.Inventory.API.IntegrationEvents;

public sealed class InventoryIntegrationEventService(
    ILogger<InventoryIntegrationEventService> logger,
    IEventBus eventBus,
    IIntegrationEventLogService eventLog)
{
    public async Task PublishAsync(IntegrationEvent integrationEvent)
    {
        try
        {
            await eventLog.MarkEventAsInProgressAsync(integrationEvent.Id);
            await eventBus.PublishAsync(integrationEvent);
            await eventLog.MarkEventAsPublishedAsync(integrationEvent.Id);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Failed to publish Inventory integration event {EventId}", integrationEvent.Id);
            await eventLog.MarkEventAsFailedAsync(integrationEvent.Id);
        }
    }
}
