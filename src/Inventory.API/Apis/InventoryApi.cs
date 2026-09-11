using eShop.Inventory.API.Application;
using Microsoft.AspNetCore.Http.HttpResults;

namespace eShop.Inventory.API.Apis;

public static class InventoryApi
{
    public static IEndpointRouteBuilder MapInventoryApi(this IEndpointRouteBuilder app)
    {
        var versionedApi = app.NewVersionedApi("Inventory");
        var api = versionedApi.MapGroup("api/inventory").HasApiVersion(1.0);

        api.MapGet("/{skuId:int}/{locationCode}", GetInventoryAsync)
            .WithName("GetInventory")
            .WithSummary("Get inventory for a SKU and location");
        api.MapPost("/reservations", ReserveAsync)
            .WithName("ReserveInventory")
            .WithSummary("Atomically reserve every order line");
        api.MapPost("/reservations/commit", CommitAsync).WithName("CommitInventorySale");
        api.MapPost("/reservations/release", ReleaseAsync).WithName("ReleaseInventory");
        api.MapPost("/restocks", RestockAsync).WithName("RestockInventory");

        return app;
    }

    private static async Task<Results<Ok<InventoryBalanceResponse>, NotFound, BadRequest<string>>> GetInventoryAsync(
        int skuId,
        string locationCode,
        IInventoryService service,
        CancellationToken cancellationToken)
    {
        if (skuId <= 0 || string.IsNullOrWhiteSpace(locationCode) || locationCode.Trim().Length > 16)
        {
            return TypedResults.BadRequest("A positive SKU ID and a location code of at most 16 characters are required.");
        }

        var balance = await service.GetAsync(skuId, locationCode, cancellationToken);
        return balance is null ? TypedResults.NotFound() : TypedResults.Ok(balance);
    }

    private static Task<IResult> ReserveAsync(ReserveInventoryRequest request, IInventoryService service, CancellationToken cancellationToken) =>
        ToHttpResultAsync(service.ReserveAsync(request, cancellationToken));

    private static Task<IResult> CommitAsync(CompleteInventoryRequest request, IInventoryService service, CancellationToken cancellationToken) =>
        ToHttpResultAsync(service.CommitAsync(request, cancellationToken));

    private static Task<IResult> ReleaseAsync(CompleteInventoryRequest request, IInventoryService service, CancellationToken cancellationToken) =>
        ToHttpResultAsync(service.ReleaseAsync(request, cancellationToken));

    private static Task<IResult> RestockAsync(RestockInventoryRequest request, IInventoryService service, CancellationToken cancellationToken) =>
        ToHttpResultAsync(service.RestockAsync(request, cancellationToken));

    private static async Task<IResult> ToHttpResultAsync(Task<InventoryOperationResult> operation)
    {
        var result = await operation;
        return result.Outcome switch
        {
            InventoryOperationOutcome.Succeeded => TypedResults.Ok(result),
            InventoryOperationOutcome.AlreadyProcessed => TypedResults.Ok(result),
            InventoryOperationOutcome.Invalid => TypedResults.BadRequest(result),
            InventoryOperationOutcome.NotFound => TypedResults.NotFound(result),
            InventoryOperationOutcome.InsufficientStock => TypedResults.Conflict(result),
            _ => TypedResults.Conflict(result)
        };
    }
}
