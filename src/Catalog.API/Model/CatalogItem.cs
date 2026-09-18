using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using Pgvector;

namespace eShop.Catalog.API.Model;

public class CatalogItem
{
    public int Id { get; set; }

    [Required]
    public string Name { get; set; }

    public string? Description { get; set; }

    public decimal Price { get; set; }

    public string? PictureFileName { get; set; }

    public int CatalogTypeId { get; set; }

    public CatalogType? CatalogType { get; set; }

    public int CatalogBrandId { get; set; }

    public CatalogBrand? CatalogBrand { get; set; }

    /// <summary>Optional embedding for the catalog item's description.</summary>
    [JsonIgnore]
    public Vector? Embedding { get; set; }

    public CatalogItem(string name) { Name = name; }
}
