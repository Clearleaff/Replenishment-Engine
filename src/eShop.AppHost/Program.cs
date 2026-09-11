using eShop.AppHost;

var builder = DistributedApplication.CreateBuilder(args);

builder.AddForwardedHeaders();
builder.AddAzureContainerAppEnvironment("aca");

var redis = builder.AddRedis("redis");
var rabbitMq = builder.AddRabbitMQ("eventbus")
    .WithLifetime(ContainerLifetime.Persistent);
var postgres = builder.AddPostgres("postgres")
    .WithImage("ankane/pgvector")
    .WithImageTag("latest")
    .WithLifetime(ContainerLifetime.Persistent);

if (Extensions.IsDataPlatformEnabled(builder.Configuration))
{
    postgres.WithArgs(
        "-c", "wal_level=logical",
        "-c", "max_replication_slots=10",
        "-c", "max_wal_senders=10");
}

var catalogDb = postgres.AddDatabase("catalogdb");
var identityDb = postgres.AddDatabase("identitydb");
var orderDb = postgres.AddDatabase("orderingdb");
var inventoryDb = postgres.AddDatabase("inventorydb");
var webhooksDb = postgres.AddDatabase("webhooksdb");

var launchProfileName = ShouldUseHttpForEndpoints() ? "http" : "https";

// Services
var identityApi = builder.AddProject<Projects.Identity_API>("identity-api", launchProfileName)
    .WithExternalHttpEndpoints()
    .WithReference(identityDb)
    .WithHttpHealthCheck("/health");

var identityEndpoint = identityApi.GetEndpoint(launchProfileName);

var basketApi = builder.AddProject<Projects.Basket_API>("basket-api")
    .WithReference(redis)
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithEnvironment("Identity__Url", identityEndpoint);
redis.WithParentRelationship(basketApi);

var catalogApi = builder.AddProject<Projects.Catalog_API>("catalog-api")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(catalogDb);

var orderingApi = builder.AddProject<Projects.Ordering_API>("ordering-api")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(orderDb).WaitFor(orderDb)
    .WithHttpHealthCheck("/health")
    .WithEnvironment("Identity__Url", identityEndpoint);

var inventoryApi = builder.AddProject<Projects.Inventory_API>("inventory-api")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(inventoryDb).WaitFor(inventoryDb)
    .WithHttpHealthCheck("/health");

var orderProcessor = builder.AddProject<Projects.OrderProcessor>("order-processor")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(orderDb)
    .WaitFor(orderingApi); // wait for the orderingApi to be ready because that contains the EF migrations

var paymentProcessor = builder.AddProject<Projects.PaymentProcessor>("payment-processor")
    .WithReference(rabbitMq).WaitFor(rabbitMq);

var webHooksApi = builder.AddProject<Projects.Webhooks_API>("webhooks-api")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(webhooksDb)
    .WithEnvironment("Identity__Url", identityEndpoint);

// Reverse proxies
builder.AddYarp("mobile-bff")
    .WithExternalHttpEndpoints()
    .ConfigureMobileBffRoutes(catalogApi, orderingApi, identityApi);

// Apps
var webhooksClient = builder.AddProject<Projects.WebhookClient>("webhooksclient", launchProfileName)
    .WithReference(webHooksApi)
    .WithEnvironment("IdentityUrl", identityEndpoint);

var webApp = builder.AddProject<Projects.WebApp>("webapp", launchProfileName)
    .WithExternalHttpEndpoints()
    .WithUrls(c => c.Urls.ForEach(u => u.DisplayText = $"Online Store ({u.Endpoint?.EndpointName})"))
    .WithReference(basketApi)
    .WithReference(catalogApi)
    .WithReference(orderingApi)
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WaitFor(identityApi)
    .WithEnvironment("IdentityUrl", identityEndpoint);

// Set UseFoundry=true to provision Microsoft Foundry for chat and embeddings.
bool useFoundry = Extensions.IsFoundryEnabled(builder.Configuration);
if (useFoundry)
{
    builder.AddFoundry(catalogApi, webApp);
}

bool useOllama = false;
if (useOllama)
{
    builder.AddOllama(catalogApi, webApp);
}

// Wire up the callback urls (self referencing)
webApp.WithEnvironment("CallBackUrl", webApp.GetEndpoint(launchProfileName));
webhooksClient.WithEnvironment("CallBackUrl", webhooksClient.GetEndpoint(launchProfileName));

// Identity has a reference to all of the apps for callback urls, this is a cyclic reference
identityApi.WithEnvironment("BasketApiClient", basketApi.GetEndpoint("http"))
           .WithEnvironment("OrderingApiClient", orderingApi.GetEndpoint("http"))
           .WithEnvironment("WebhooksApiClient", webHooksApi.GetEndpoint("http"))
           .WithEnvironment("WebhooksWebClient", webhooksClient.GetEndpoint(launchProfileName))
           .WithEnvironment("WebAppClient", webApp.GetEndpoint(launchProfileName));

if (Extensions.IsDataPlatformEnabled(builder.Configuration))
{
    builder.AddDataPlatform(postgres, inventoryApi);
}

if (Extensions.IsOrderGeneratorEnabled(builder.Configuration))
{
    var clientSecret = builder.AddParameter("order-generator-client-secret", secret: true);
    identityApi.WithEnvironment("OrderGenerator__ClientSecret", clientSecret);
    orderProcessor
        .WithEnvironment("BackgroundTaskOptions__GracePeriodTime", "0")
        .WithEnvironment("BackgroundTaskOptions__CheckUpdateTime", "1");
    paymentProcessor.WithEnvironment("PaymentOptions__PaymentSucceeded", "true");
    orderingApi.WithEnvironment("OrderingProcessing__SimulatedDelayMilliseconds", "0");

    builder.AddProject<Projects.OrderLoadGenerator>("order-load-generator")
        .WithReference(identityApi).WaitFor(identityApi)
        .WithReference(catalogApi).WaitFor(catalogApi)
        .WithReference(orderingApi).WaitFor(orderingApi)
        .WithEnvironment("ORDERGEN_IDENTITY_URL", identityEndpoint)
        .WithEnvironment("ORDERGEN_CATALOG_URL", catalogApi.GetEndpoint("http"))
        .WithEnvironment("ORDERGEN_ORDERING_URL", orderingApi.GetEndpoint("http"))
        .WithEnvironment("ORDERGEN_CLIENT_SECRET", clientSecret)
        .WithEnvironment("ORDERGEN_RATE_PER_SECOND", builder.Configuration["OrderGenerator:RatePerSecond"] ?? "10")
        .WithEnvironment("ORDERGEN_DURATION_SECONDS", builder.Configuration["OrderGenerator:DurationSeconds"] ?? "60")
        .WithEnvironment("ORDERGEN_MAX_CONCURRENCY", builder.Configuration["OrderGenerator:MaxConcurrency"] ?? "50")
        .WithEnvironment("ORDERGEN_RANDOM_SEED", builder.Configuration["OrderGenerator:RandomSeed"] ?? "42")
        .WithEnvironment("ORDERGEN_PROFILE", builder.Configuration["OrderGenerator:Profile"] ?? "NORMAL")
        .WithEnvironment("ORDERGEN_TARGET_SKU_ID", builder.Configuration["OrderGenerator:TargetSkuId"] ?? "42")
        .WithEnvironment("ORDERGEN_SIMULATED_DAY_OF_WEEK", builder.Configuration["OrderGenerator:SimulatedDayOfWeek"] ?? "-1");
}

builder.Build().Run();

// For test use only.
// Looks for an environment variable that forces the use of HTTP for all the endpoints. We
// are doing this for ease of running the Playwright tests in CI.
static bool ShouldUseHttpForEndpoints()
{
    const string EnvVarName = "ESHOP_USE_HTTP_ENDPOINTS";
    var envValue = Environment.GetEnvironmentVariable(EnvVarName);

    // Attempt to parse the environment variable value; return true if it's exactly "1".
    return int.TryParse(envValue, out int result) && result == 1;
}
