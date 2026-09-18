using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using eShop.OrderLoadGenerator;
using eShop.OrderLoadGenerator.Authentication;
using eShop.OrderLoadGenerator.Catalog;
using eShop.OrderLoadGenerator.Generation;
using eShop.OrderLoadGenerator.Ordering;
using eShop.OrderLoadGenerator.Telemetry;
using eShop.ServiceDefaults;

var builder = Host.CreateApplicationBuilder(args);
AddOrderGeneratorEnvironmentOverrides(builder.Configuration);
builder.AddBasicServiceDefaults();

builder.Services.AddOptions<OrderGeneratorOptions>()
    .BindConfiguration(OrderGeneratorOptions.SectionName)
    .ValidateDataAnnotations()
    .Validate(settings => Uri.TryCreate(settings.IdentityBaseUrl, UriKind.Absolute, out _), "IdentityBaseUrl must be an absolute URL.")
    .Validate(settings => Uri.TryCreate(settings.CatalogBaseUrl, UriKind.Absolute, out _), "CatalogBaseUrl must be an absolute URL.")
    .Validate(settings => Uri.TryCreate(settings.OrderingBaseUrl, UriKind.Absolute, out _), "OrderingBaseUrl must be an absolute URL.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddHttpClient("identity", (services, client) =>
    client.BaseAddress = CreateBaseAddress(services.GetRequiredService<Microsoft.Extensions.Options.IOptions<OrderGeneratorOptions>>().Value.IdentityBaseUrl));
builder.Services.AddHttpClient("catalog", (services, client) =>
    client.BaseAddress = CreateBaseAddress(services.GetRequiredService<Microsoft.Extensions.Options.IOptions<OrderGeneratorOptions>>().Value.CatalogBaseUrl));
builder.Services.AddHttpClient("ordering", (services, client) =>
    client.BaseAddress = CreateBaseAddress(services.GetRequiredService<Microsoft.Extensions.Options.IOptions<OrderGeneratorOptions>>().Value.OrderingBaseUrl));

builder.Services.AddSingleton<IAccessTokenProvider, AccessTokenProvider>();
builder.Services.AddSingleton<ICatalogSnapshotProvider, CatalogSnapshotProvider>();
builder.Services.AddSingleton<IOrderingClient, OrderingClient>();
builder.Services.AddSingleton<LoadRunStatistics>();
builder.Services.AddSingleton<OrderLoadRunner>();
builder.Services.AddHostedService<OrderRateWorker>();

await builder.Build().RunAsync();

static Uri CreateBaseAddress(string value) => new(value.TrimEnd('/') + '/');

static void AddOrderGeneratorEnvironmentOverrides(ConfigurationManager configuration)
{
    var mappings = new Dictionary<string, string>
    {
        ["ORDERGEN_RATE_PER_SECOND"] = "RatePerSecond",
        ["ORDERGEN_MAXIMUM_RATE_PER_SECOND"] = "MaximumRatePerSecond",
        ["ORDERGEN_MINIMUM_RATE_PER_SECOND"] = "MinimumRatePerSecond",
        ["ORDERGEN_WAVE_PERIOD_SECONDS"] = "WavePeriodSeconds",
        ["ORDERGEN_WAVE_AMPLITUDE_FRACTION"] = "WaveAmplitudeFraction",
        ["ORDERGEN_VARIABLE_RATE"] = "VariableRate",
        ["ORDERGEN_CONTINUOUS"] = "Continuous",
        ["ORDERGEN_DURATION_SECONDS"] = "DurationSeconds",
        ["ORDERGEN_MAX_CONCURRENCY"] = "MaxConcurrency",
        ["ORDERGEN_RANDOM_SEED"] = "RandomSeed",
        ["ORDERGEN_PROFILE"] = "Profile",
        ["ORDERGEN_TARGET_SKU_ID"] = "TargetSkuId",
        ["ORDERGEN_SIMULATED_DAY_OF_WEEK"] = "SimulatedDayOfWeek",
        ["ORDERGEN_CUSTOMER_POOL_SIZE"] = "CustomerPoolSize",
        ["ORDERGEN_REQUEST_TIMEOUT_SECONDS"] = "RequestTimeoutSeconds",
        ["ORDERGEN_TRANSIENT_RETRIES"] = "TransientRetries",
        ["ORDERGEN_DRAIN_TIMEOUT_SECONDS"] = "DrainTimeoutSeconds",
        ["ORDERGEN_IDENTITY_URL"] = "IdentityBaseUrl",
        ["ORDERGEN_CATALOG_URL"] = "CatalogBaseUrl",
        ["ORDERGEN_ORDERING_URL"] = "OrderingBaseUrl",
        ["ORDERGEN_CLIENT_ID"] = "ClientId",
        ["ORDERGEN_CLIENT_SECRET"] = "ClientSecret",
        ["ORDERGEN_SCOPE"] = "Scope"
    };
    var overrides = mappings
        .Select(pair => (Key: $"{OrderGeneratorOptions.SectionName}:{pair.Value}", Value: Environment.GetEnvironmentVariable(pair.Key)))
        .Where(pair => !string.IsNullOrWhiteSpace(pair.Value))
        .Select(pair => pair.Key.EndsWith(":Profile", StringComparison.Ordinal)
            ? (pair.Key, Value: OrderWorkloadProfiles.Normalize(pair.Value!))
            : pair)
        .ToDictionary(pair => pair.Key, pair => pair.Value);
    configuration.AddInMemoryCollection(overrides!);
}
