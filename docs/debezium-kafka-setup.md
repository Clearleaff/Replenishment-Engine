# Debezium and Kafka Local Setup

## Runtime flow

```text
Inventory.API -> PostgreSQL inventorydb -> logical WAL/slot
              -> Debezium Kafka Connect -> two Kafka topics -> Rust consumer
```

PostgreSQL's write-ahead log (WAL) is the durable change stream. Debezium reads it through replication slot `eshop_inventory_cdc`, creates/uses publication `eshop_inventory_publication`, and publishes only the two tables defined in [cdc-contract.md](cdc-contract.md). Kafka retains those records independently of consumer uptime.

## Files, in implementation order

### `Directory.Packages.props`

Why: central package management requires every NuGet version in one repository-level file. The change added `Aspire.Hosting.Kafka` at the existing `AspireVersion`; projects still cannot choose a drifting Kafka integration version locally.

### `src/eShop.AppHost/eShop.AppHost.csproj`

Why: AppHost needs the Kafka hosting extension at compile time. The new package reference has no `Version` because the preceding central entry supplies it.

### `src/eShop.AppHost/Extensions.cs`

Why: the accepted application should remain lightweight unless CDC is requested. `IsDataPlatformEnabled` reads `DataPlatform:Enabled`, defaults false, and allows tests to prove the opt-in gate without launching containers.

### `src/eShop.AppHost/Program.cs`

Important changes, sequentially:

1. When the data platform is enabled, PostgreSQL receives `-c wal_level=logical`, `-c max_replication_slots=10`, and `-c max_wal_senders=10`.
2. Existing PostgreSQL and Inventory resources remain the source/authority.
3. `AddDataPlatform(postgres, inventoryApi)` attaches Kafka, Connect, ClickHouse, registrar, and Rust only behind the flag.

The settings were also applied to the existing persistent PostgreSQL container with `ALTER SYSTEM`, followed by a non-destructive restart. Verification returned `logical|10|10`; the named volume and existing Inventory rows remained intact.

### `src/eShop.AppHost/DataPlatformExtensions.cs`

Responsibility: describes all new local resources and their dependency order.

Sequential behavior:

1. `AddKafka("kafka")` uses Aspire's KRaft-capable local Kafka image, a named data volume, and persistent lifetime.
2. `debezium-connect` pins `quay.io/debezium/connect:3.6.2.Final`, points its internal/config/offset/status topics at Kafka, exposes port 8083, and waits until Kafka is healthy.
3. `clickhouse` pins `clickhouse:26.8.2.7`, creates logical database `eshop_analytics`, injects a secret password parameter, exposes HTTP/native endpoints, and persists `/var/lib/clickhouse`.
4. `inventory-connector-registration` runs the repository script after Connect and PostgreSQL are ready.
5. The Rust executable receives external Kafka/ClickHouse/Inventory endpoints and mode/lead-time configuration. Its dashboard is deliberately non-proxied on local port 8088.
6. `WaitForCompletion(connectorRegistration)` matters: registration is a one-shot job. Waiting for `Running` would deadlock on a later restart after the job had already finished.

Dependencies flow downward; Inventory never receives a dependency on analytics.

### `infra/cdc/register-inventory-connector.sh`

Why: connector registration must be repeatable and reviewable instead of a manual dashboard click.

Line-by-line logic:

1. `set -euo pipefail` fails on command errors, unset variables, or failed pipelines.
2. `:${VAR:?...}` checks every URL/database credential before doing work.
3. `${CONNECT_URL%/}` prevents a double slash.
4. `jq -n` creates JSON without unsafe string concatenation and injects the password only at runtime.
5. The connector uses PostgreSQL `pgoutput`, the stable slot/publication names, `snapshot.mode=initial`, and an exact two-table allow-list.
6. Connector-level JSON converters disable schemas for both keys and values. The decoder remains compatible with older wrapped records.
7. `curl --request PUT .../config` makes registration idempotent: first call creates configuration; later calls update it.

Credentials are never committed or printed by the script.

### `tests/eShop.AppHost.UnitTests/AppHostConfigurationTests.cs`

The added data-driven test verifies common true/false spellings for the feature flag. This catches an accidental always-on data stack without requiring Docker.

## Local commands

```bash
Parameters__clickhouse-password=local-dev-only \
DataPlatform__Enabled=true \
DataPlatform__AgentMode=OBSERVE \
ESHOP_USE_HTTP_ENDPOINTS=1 \
aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj

aspire wait rust-data-platform --timeout 180
curl http://127.0.0.1:8088/health
```

Use `aspire describe <resource> --format Json` to discover dynamic local ports. Do not copy emitted development credentials into source control.

## Verified checkpoint

- AppHost build: succeeded with zero warnings/errors.
- Connector and its task: `RUNNING`, Debezium `3.6.2.Final`.
- Kafka end offsets after the smoke change: movements `3151`, balances `405`.
- The exact last records showed the same RESTOCK and newer balance version.
- Rust health after compatibility replay reported normalized records and zero new dead letters.

## Failure modes

- `wal_level` not logical: connector cannot stream; check `SHOW wal_level` and restart PostgreSQL after changing it.
- Slot retained while consumer infrastructure is down: WAL grows; monitor slot lag.
- Connector registration failure: inspect registrar and Connect logs, then safely rerun its idempotent PUT.
- Schema wrapper mismatch: supported by the decoder regression test; malformed payloads go to `cdc_dead_letters`.
- Kafka unavailable: Connect/Rust wait or retry; no Inventory transaction is rolled back by an analytics outage.
