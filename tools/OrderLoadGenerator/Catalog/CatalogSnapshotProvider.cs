using System.Net.Http.Json;
using Microsoft.Extensions.Options;
using eShop.OrderLoadGenerator.Generation;

namespace eShop.OrderLoadGenerator.Catalog;

public interface ICatalogSnapshotProvider
{
    Task<IReadOnlyList<CatalogProduct>> GetProductsAsync(CancellationToken cancellationToken);
}

public sealed class CatalogSnapshotProvider(
    IHttpClientFactory httpClientFactory,
    IOptions<OrderGeneratorOptions> options) : ICatalogSnapshotProvider
{
    public async Task<IReadOnlyList<CatalogProduct>> GetProductsAsync(CancellationToken cancellationToken)
    {
        var pageSize = options.Value.CatalogPageSize;
        var path = $"api/catalog/items?api-version=1.0&pageSize={pageSize}&pageIndex=0";
        var page = await httpClientFactory.CreateClient("catalog")
            .GetFromJsonAsync<CatalogPage>(path, cancellationToken)
            ?? throw new InvalidOperationException("Catalog returned an empty response.");

        var products = page.Data
            .Where(product => product.Id > 0 && !string.IsNullOrWhiteSpace(product.Name) && product.Price >= 0)
            .OrderBy(product => product.Id)
            .ToArray();
        if (products.Length == 0)
        {
            throw new InvalidOperationException("Catalog returned no usable products.");
        }

        if (page.Count > page.Data.Count)
        {
            throw new InvalidOperationException(
                $"Catalog contains {page.Count} products but the one-request snapshot received only {page.Data.Count}. Increase CatalogPageSize.");
        }

        return products;
    }

    private sealed record CatalogPage(long Count, IReadOnlyList<CatalogProduct> Data);
}
