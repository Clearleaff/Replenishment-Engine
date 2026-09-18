namespace eShop.OrderLoadGenerator.Generation;

public sealed record CatalogProduct(int Id, string Name, decimal Price, string? PictureFileName = null);

public sealed record GeneratedOrderItem(
    string Id,
    int ProductId,
    string ProductName,
    decimal UnitPrice,
    decimal OldUnitPrice,
    int Quantity,
    string? PictureUrl);

public sealed record OrderScenario(
    Guid RequestId,
    string CustomerId,
    string CustomerName,
    string LocationCode,
    IReadOnlyList<GeneratedOrderItem> Items)
{
    public int TotalUnits => Items.Sum(item => item.Quantity);
}

public sealed record CreateOrderPayload(
    string UserId,
    string UserName,
    string City,
    string Street,
    string State,
    string Country,
    string ZipCode,
    string CardNumber,
    string CardHolderName,
    DateTime CardExpiration,
    string CardSecurityNumber,
    int CardTypeId,
    string Buyer,
    IReadOnlyList<GeneratedOrderItem> Items,
    string LocationCode);
