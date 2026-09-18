using Aspire.Hosting.ApplicationModel;

namespace eShop.AppHost;

internal static class DataPlatformExtensions
{
    public static void AddDataPlatform(
        this IDistributedApplicationBuilder builder,
        IResourceBuilder<PostgresServerResource> postgres,
        IResourceBuilder<ProjectResource> inventoryApi,
        IResourceBuilder<PostgresDatabaseResource> governanceDb)
    {
        var kafka = builder.AddKafka("kafka")
            .WithDataVolume()
            .WithLifetime(ContainerLifetime.Persistent);

        var connect = builder.AddContainer("debezium-connect", "quay.io/debezium/connect", "3.6.2.Final")
            .WithEnvironment(
                "BOOTSTRAP_SERVERS",
                kafka.Resource.InternalEndpoint.Property(EndpointProperty.HostAndPort))
            .WithEnvironment("GROUP_ID", "eshop-inventory-connect")
            .WithEnvironment("CONFIG_STORAGE_TOPIC", "eshop_connect_configs")
            .WithEnvironment("OFFSET_STORAGE_TOPIC", "eshop_connect_offsets")
            .WithEnvironment("STATUS_STORAGE_TOPIC", "eshop_connect_statuses")
            .WithEnvironment("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .WithEnvironment("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .WithEnvironment("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .WithEnvironment("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .WithHttpEndpoint(targetPort: 8083, name: "http")
            .WithHttpHealthCheck("/connectors")
            .WithReference(kafka)
            .WaitFor(kafka);

        var clickHousePassword = builder.AddParameter("clickhouse-password", secret: true);
        var clickHouse = builder.AddContainer("clickhouse", "clickhouse", "26.8.2.7")
            .WithEnvironment("CLICKHOUSE_DB", "eshop_analytics")
            .WithEnvironment("CLICKHOUSE_USER", "eshop")
            .WithEnvironment("CLICKHOUSE_PASSWORD", clickHousePassword)
            .WithHttpEndpoint(targetPort: 8123, name: "http")
            .WithEndpoint(targetPort: 9000, name: "native")
            .WithHttpHealthCheck("/ping")
            .WithVolume("eshop-clickhouse-data", "/var/lib/clickhouse")
            .WithLifetime(ContainerLifetime.Persistent);

        var connectorRegistration = builder.AddExecutable(
                "inventory-connector-registration",
                "bash",
                Path.GetFullPath(Path.Combine(builder.AppHostDirectory, "..", "..")),
                "infra/cdc/register-inventory-connector.sh")
            .WithEnvironment("CONNECT_URL", connect.GetEndpoint("http"))
            .WithEnvironment("INVENTORY_DB_HOST", "postgres")
            .WithEnvironment("INVENTORY_DB_PORT", "5432")
            .WithEnvironment("INVENTORY_DB_NAME", "inventorydb")
            .WithEnvironment("INVENTORY_DB_USER", postgres.Resource.UserNameReference)
            .WithEnvironment("INVENTORY_DB_PASSWORD", postgres.Resource.PasswordParameter)
            .WaitFor(connect)
            .WaitFor(postgres);

        builder.AddExecutable(
                "rust-data-platform",
                "cargo",
                Path.GetFullPath(Path.Combine(builder.AppHostDirectory, "..", "..")),
                "run",
                "--manifest-path",
                "src/RustDataPlatform/Cargo.toml",
                "-p",
                "cdc-consumer")
            .WithEnvironment(
                "KAFKA_BOOTSTRAP_SERVERS",
                kafka.Resource.PrimaryEndpoint.Property(EndpointProperty.HostAndPort))
            .WithEnvironment("KAFKA_CONSUMER_GROUP", "eshop-rust-cdc-v3")
            .WithEnvironment("CLICKHOUSE_URL", clickHouse.GetEndpoint("http"))
            .WithEnvironment("CLICKHOUSE_DATABASE", "eshop_analytics")
            .WithEnvironment("CLICKHOUSE_USER", "eshop")
            .WithEnvironment("CLICKHOUSE_PASSWORD", clickHousePassword)
            .WithEnvironment("INVENTORY_API_URL", inventoryApi.GetEndpoint("http"))
            .WithReference(governanceDb)
            .WithEnvironment("DASHBOARD_BIND", "127.0.0.1:8088")
            .WithEnvironment("AGENT_MODE", builder.Configuration["DataPlatform:AgentMode"] ?? "OBSERVE")
            .WithEnvironment("ESHOP_ENVIRONMENT", "Development")
            .WithEnvironment(
                "SIMULATED_LEAD_TIME_SECONDS",
                builder.Configuration["DataPlatform:SimulatedLeadTimeSeconds"] ?? "15")
            .WithEnvironment(
                "DATA_LAKE_ROOT",
                builder.Configuration["DataPlatform:DataLakeRoot"]
                    ?? Path.GetFullPath(Path.Combine(builder.AppHostDirectory, "..", "..", "data-lake")))
            .WithHttpEndpoint(targetPort: 8088, name: "http", isProxied: false)
            .WithHttpHealthCheck("/health")
            .WaitForCompletion(connectorRegistration)
            .WaitFor(governanceDb)
            .WaitFor(clickHouse)
            .WaitFor(inventoryApi);
    }
}
