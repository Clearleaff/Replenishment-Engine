namespace eShop.Inventory.API.Application;

public interface IInventoryService
{
    Task<InventoryBalanceResponse?> GetAsync(int skuId, string locationCode, CancellationToken cancellationToken);
    Task<InventoryOperationResult> ReserveAsync(ReserveInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> CommitAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> ReleaseAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> RestockAsync(RestockInventoryRequest request, CancellationToken cancellationToken = default);
}
