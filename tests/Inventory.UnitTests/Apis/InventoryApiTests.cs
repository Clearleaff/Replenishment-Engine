using System.Net;
using System.Net.Http.Json;
using eShop.Inventory.API.Apis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace eShop.Inventory.UnitTests.Apis;

[TestClass]
public sealed class InventoryApiTests
{
    [TestMethod]
    public async Task Get_returns_known_normalized_balance_and_expected_errors()
    {
        await using var app = await StartAppAsync(new StubInventoryService());
        var client = app.GetTestClient();

        var found = await client.GetAsync("/api/inventory/1/ncr");
        var response = await found.Content.ReadFromJsonAsync<InventoryBalanceResponse>();
        Assert.AreEqual(HttpStatusCode.OK, found.StatusCode);
        Assert.AreEqual("NCR", response!.LocationCode);
        Assert.AreEqual(response.OnHand - response.Reserved, response.Available);
        Assert.AreEqual(HttpStatusCode.NotFound, (await client.GetAsync("/api/inventory/99/NCR")).StatusCode);
        Assert.AreEqual(HttpStatusCode.BadRequest, (await client.GetAsync("/api/inventory/0/NCR")).StatusCode);
    }

    [TestMethod]
    public async Task Reserve_maps_insufficient_stock_to_conflict()
    {
        await using var app = await StartAppAsync(new StubInventoryService());
        var response = await app.GetTestClient().PostAsJsonAsync(
            "/api/inventory/reservations",
            new ReserveInventoryRequest(Guid.NewGuid(), 1, "NCR", [new(1, 999)]));

        Assert.AreEqual(HttpStatusCode.Conflict, response.StatusCode);
    }

    private static async Task<WebApplication> StartAppAsync(IInventoryService inventory)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddApiVersioning(options =>
        {
            options.DefaultApiVersion = new Asp.Versioning.ApiVersion(1, 0);
            options.AssumeDefaultVersionWhenUnspecified = true;
        });
        builder.Services.AddSingleton(inventory);
        var app = builder.Build();
        app.MapInventoryApi();
        await app.StartAsync();
        return app;
    }

    private sealed class StubInventoryService : IInventoryService
    {
        public Task<InventoryBalanceResponse?> GetAsync(int skuId, string locationCode, CancellationToken cancellationToken) =>
            Task.FromResult(skuId == 1
                ? new InventoryBalanceResponse(1, locationCode.Trim().ToUpperInvariant(), 10, 2, 8, 2, 5, 20, 3, DateTime.UtcNow)
                : null);

        public Task<InventoryOperationResult> ReserveAsync(ReserveInventoryRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new InventoryOperationResult(InventoryOperationOutcome.InsufficientStock, "insufficient", [1]));

        public Task<InventoryOperationResult> CommitAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new InventoryOperationResult(InventoryOperationOutcome.Succeeded, "committed"));

        public Task<InventoryOperationResult> ReleaseAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new InventoryOperationResult(InventoryOperationOutcome.Succeeded, "released"));

        public Task<InventoryOperationResult> RestockAsync(RestockInventoryRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new InventoryOperationResult(InventoryOperationOutcome.Succeeded, "restocked"));
    }
}
