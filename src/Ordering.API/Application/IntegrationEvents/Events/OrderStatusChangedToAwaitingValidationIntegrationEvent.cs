namespace eShop.Ordering.API.Application.IntegrationEvents.Events;

public record OrderStatusChangedToAwaitingValidationIntegrationEvent : IntegrationEvent
{
    public int OrderId { get; }
    public OrderStatus OrderStatus { get; }
    public string BuyerName { get; }
    public string BuyerIdentityGuid { get; }
    public string LocationCode { get; }
    public IEnumerable<OrderStockItem> OrderStockItems { get; }

    public OrderStatusChangedToAwaitingValidationIntegrationEvent(
        int orderId, OrderStatus orderStatus, string buyerName, string buyerIdentityGuid, string locationCode,
        IEnumerable<OrderStockItem> orderStockItems)
    {
        OrderId = orderId;
        OrderStockItems = orderStockItems;
        OrderStatus = orderStatus;
        BuyerName = buyerName;
        BuyerIdentityGuid = buyerIdentityGuid;
        LocationCode = locationCode;
    }
}

public record OrderStockItem
{
    public int ProductId { get; }
    public int Units { get; }

    public OrderStockItem(int productId, int units)
    {
        ProductId = productId;
        Units = units;
    }
}
