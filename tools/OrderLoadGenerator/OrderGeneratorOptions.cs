using System.ComponentModel.DataAnnotations;

namespace eShop.OrderLoadGenerator;

public enum OrderWorkloadProfile
{
    Normal,
    WeekdaySeasonal,
    Promotion,
    Viral,
    RegionalSpike
}

public static class OrderWorkloadProfiles
{
    public static string Normalize(string value) => value.Trim().ToUpperInvariant() switch
    {
        "NORMAL" => nameof(OrderWorkloadProfile.Normal),
        "WEEKDAY_SEASONAL" or "WEEKDAYSEASONAL" => nameof(OrderWorkloadProfile.WeekdaySeasonal),
        "PROMOTION" => nameof(OrderWorkloadProfile.Promotion),
        "VIRAL" => nameof(OrderWorkloadProfile.Viral),
        "REGIONAL_SPIKE" or "REGIONALSPIKE" => nameof(OrderWorkloadProfile.RegionalSpike),
        _ => throw new FormatException($"Unknown order workload profile '{value}'.")
    };
}

public sealed class OrderGeneratorOptions
{
    public const string SectionName = "OrderGenerator";

    [Range(0.01, 10_000)]
    public double RatePerSecond { get; set; } = 10;

    [Range(0.1, 86_400)]
    public double DurationSeconds { get; set; } = 60;

    [Range(1, 10_000)]
    public int MaxConcurrency { get; set; } = 50;

    public int RandomSeed { get; set; } = 42;

    [EnumDataType(typeof(OrderWorkloadProfile))]
    public OrderWorkloadProfile Profile { get; set; } = OrderWorkloadProfile.Normal;

    [Range(1, 100_000)]
    public int TargetSkuId { get; set; } = 42;

    [Range(-1, 6)]
    public int SimulatedDayOfWeek { get; set; } = -1;

    [Range(1, 100_000)]
    public int CustomerPoolSize { get; set; } = 200;

    [Range(1, 10_000)]
    public int CatalogPageSize { get; set; } = 1_000;

    [Range(0.01, 1)]
    public double HotSkuFraction { get; set; } = 0.2;

    [Range(0, 1)]
    public double HotTrafficShare { get; set; } = 0.8;

    [Range(0.1, 300)]
    public double RequestTimeoutSeconds { get; set; } = 10;

    [Range(0, 10)]
    public int TransientRetries { get; set; } = 1;

    [Range(1, 600)]
    public double DrainTimeoutSeconds { get; set; } = 30;

    [Required]
    public string IdentityBaseUrl { get; set; } = string.Empty;

    [Required]
    public string CatalogBaseUrl { get; set; } = string.Empty;

    [Required]
    public string OrderingBaseUrl { get; set; } = string.Empty;

    [Required]
    public string ClientId { get; set; } = "order-generator";

    [Required]
    public string ClientSecret { get; set; } = string.Empty;

    public string Scope { get; set; } = "orders";
}
