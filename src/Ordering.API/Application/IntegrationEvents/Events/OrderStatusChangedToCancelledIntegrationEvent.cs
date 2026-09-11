namespace eShop.Ordering.API.Application.IntegrationEvents.Events;

public record OrderStatusChangedToCancelledIntegrationEvent : IntegrationEvent
{
    public int OrderId { get; }
    public OrderStatus OrderStatus { get; }
    public string BuyerName { get; }
    public string BuyerIdentityGuid { get; }
    public string LocationCode { get; }

    public OrderStatusChangedToCancelledIntegrationEvent
        (int orderId, OrderStatus orderStatus, string buyerName, string buyerIdentityGuid, string locationCode)
    {
        OrderId = orderId;
        OrderStatus = orderStatus;
        BuyerName = buyerName;
        BuyerIdentityGuid = buyerIdentityGuid;
        LocationCode = locationCode;
    }
}
