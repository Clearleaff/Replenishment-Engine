namespace eShop.Inventory.API.Application;

public sealed record InventoryItemRequest(int SkuId, int Quantity);

public sealed record ReserveInventoryRequest(
    Guid OperationId,
    int OrderId,
    string LocationCode,
    IReadOnlyCollection<InventoryItemRequest> Items);

public sealed record CompleteInventoryRequest(Guid OperationId, int OrderId, string LocationCode);

public sealed record RestockInventoryRequest(
    Guid OperationId,
    int SkuId,
    string LocationCode,
    int Quantity,
    string? Reason);

public sealed record InventoryBalanceResponse(
    int SkuId,
    string LocationCode,
    int OnHand,
    int Reserved,
    int Available,
    int SafetyStock,
    int ReorderPoint,
    int MaxStock,
    long Version,
    DateTime UpdatedAt);

public enum InventoryOperationOutcome
{
    Succeeded,
    AlreadyProcessed,
    Invalid,
    NotFound,
    InsufficientStock,
    Conflict
}

public sealed record InventoryOperationResult(
    InventoryOperationOutcome Outcome,
    string Message,
    IReadOnlyCollection<int>? UnavailableSkuIds = null)
{
    public bool IsSuccess => Outcome is InventoryOperationOutcome.Succeeded or InventoryOperationOutcome.AlreadyProcessed;
}
