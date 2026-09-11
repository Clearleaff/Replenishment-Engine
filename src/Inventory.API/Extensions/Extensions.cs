internal static class Extensions
{
    public static void AddApplicationServices(this IHostApplicationBuilder builder)
    {
        builder.AddNpgsqlDbContext<InventoryContext>("inventorydb");
        builder.Services.AddScoped<InventoryContextSeed>();
        builder.Services.AddScoped<IInventoryService, InventoryService>();
        builder.Services.AddHostedService<InventoryDatabaseInitializer>();
        builder.Services.AddTransient<IIntegrationEventLogService, IntegrationEventLogService<InventoryContext>>();
        builder.Services.AddTransient<InventoryIntegrationEventService>();
        builder.Services.AddScoped<InventoryIntegrationEventProcessor>();
        builder.Services.AddOptions<InventoryOptions>().BindConfiguration(InventoryOptions.SectionName);

        builder.AddRabbitMqEventBus("eventbus")
            .AddSubscription<OrderStatusChangedToAwaitingValidationIntegrationEvent, OrderStatusChangedToAwaitingValidationIntegrationEventHandler>()
            .AddSubscription<OrderStatusChangedToPaidIntegrationEvent, OrderStatusChangedToPaidIntegrationEventHandler>()
            .AddSubscription<OrderStatusChangedToCancelledIntegrationEvent, OrderStatusChangedToCancelledIntegrationEventHandler>()
            .AddSubscription<OrderStockConfirmedIntegrationEvent, OrderStockConfirmedIntegrationEventHandler>()
            .AddSubscription<OrderStockRejectedIntegrationEvent, OrderStockRejectedIntegrationEventHandler>();
    }
}
