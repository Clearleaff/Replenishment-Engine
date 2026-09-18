# Adaptive Replenishment Demo

## Workload profile extension

The accepted NORMAL generator behavior remains the default. Only controlled profile selection was added.

### `tools/OrderLoadGenerator/OrderGeneratorOptions.cs`

`OrderWorkloadProfile` defines `Normal`, `WeekdaySeasonal`, `Promotion`, `Viral`, and `RegionalSpike`. `Profile` defaults to Normal, `TargetSkuId` defaults to 42, and `SimulatedDayOfWeek=-1` means use the real UTC weekday; values 0–6 allow deterministic seasonal demos.

`OrderWorkloadProfiles.Normalize` maps the documented uppercase names (including `WEEKDAY_SEASONAL` and `REGIONAL_SPIKE`) to .NET enum names before option binding. Unknown names fail clearly. This was added after the first live profile run proved that .NET's default enum converter does not remove underscores.

### `tools/OrderLoadGenerator/Generation/OrderScenarioFactory.cs`

Normal follows the original hot-SKU and location distributions without extra random draws. Non-normal profiles require the configured Catalog SKU:

- WeekdaySeasonal includes it roughly 80% on Sunday, 10% on Tuesday, 35% otherwise.
- Promotion targets it roughly 65% with 2–3 units.
- Viral targets it roughly 90% with 3 units.
- RegionalSpike targets it roughly 90% with 2–3 units and routes every order to NCR.

Other lines retain valid unique products and the accepted payload shape. Missing target SKU fails preflight clearly instead of generating misleading traffic.
`
### `tools/OrderLoadGenerator/Program.cs

Environment mappings add `ORDERGEN_PROFILE`, `ORDERGEN_TARGET_SKU_ID`, and `ORDERGEN_SIMULATED_DAY_OF_WEEK`. Existing variables are unchanged.

### `OrderRateWorker.cs`, `LoadRunStatistics.cs`, and `appsettings.json`

Startup/summary output includes the chosen profile, and configuration explicitly records Normal defaults. This makes demo evidence self-identifying.

### `src/eShop.AppHost/Program.cs`

When the already opt-in generator resource is enabled, AppHost now forwards the three profile settings alongside its accepted rate/duration/seed settings. This lets the controlled profile run inside the same service-discovery and credentials boundary; the generator remains absent when its original feature flag is false.

### `tests/OrderLoadGenerator.UnitTests/OrderScenarioFactoryTests.cs`

New deterministic tests assert RegionalSpike is entirely NCR and strongly concentrated on SKU 42, and that WeekdaySeasonal Sunday traffic exceeds Tuesday by more than 3x. Together with existing baseline tests, 10 generator tests pass.

## Start OBSERVE mode

```bash
Parameters__clickhouse-password=local-dev-only \
DataPlatform__Enabled=true \
DataPlatform__AgentMode=OBSERVE \
ESHOP_USE_HTTP_ENDPOINTS=1 \
aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj
```

Check `curl http://127.0.0.1:8088/health` and the SKU detail route before traffic.

## Run the accepted 10/sec workload

Start the generator through its AppHost resource or run it with the same Identity/Catalog/Ordering URLs and client secret shown by `aspire describe order-load-generator --format Json`:

```bash
ORDERGEN_PROFILE=NORMAL \
ORDERGEN_RATE_PER_SECOND=10 \
ORDERGEN_DURATION_SECONDS=60 \
dotnet run --project tools/OrderLoadGenerator/OrderLoadGenerator.csproj
```

Expected accepted-baseline shape is 600 offered in 60 seconds. Always use the printed summary as evidence; HTTP acceptance is not the same as eventual Paid status.

## Controlled SKU 42 / NCR spike and closed loop

Restart AppHost with Development-only automatic mode and short simulated lead time:

```bash
Parameters__clickhouse-password=local-dev-only \
Parameters__order-generator-client-secret=local-dev-generator-only \
DataPlatform__Enabled=true \
DataPlatform__AgentMode=AUTO_DEMO \
DataPlatform__SimulatedLeadTimeSeconds=15 \
OrderGenerator__Enabled=true \
OrderGenerator__Profile=REGIONAL_SPIKE \
OrderGenerator__TargetSkuId=42 \
OrderGenerator__RatePerSecond=10 \
OrderGenerator__DurationSeconds=60 \
ESHOP_USE_HTTP_ENDPOINTS=1 \
aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj
```

Observe repeatedly:

```bash
curl http://127.0.0.1:8088/api/sku-locations/42/NCR
curl http://127.0.0.1:8088/health
```

Expected sequence: Sale windows/velocity rise, spike score and adaptive forecast rise, ETA shortens/risk rises, positive recommendation creates one open supply item, simulator calls Inventory after the lead time, on-hand/available rises through CDC feedback, and risk falls. The simulator uses an idempotent operation ID and cannot exceed Inventory `max_stock` acceptance.

## Warehouse/data correctness queries

Run these with ClickHouse client credentials supplied by Aspire:

```sql
SELECT count(), uniqExact(movement_id) FROM eshop_analytics.fact_inventory_movements FINAL;
SELECT sku_id, location_code, version, on_hand, reserved, available
FROM eshop_analytics.inventory_balance_current FINAL
WHERE sku_id = 42 AND location_code = 'NCR';
SELECT * FROM eshop_analytics.reorder_decisions
WHERE sku_id = 42 AND location_code = 'NCR'
ORDER BY created_at DESC LIMIT 5;
```

Build the offline Gold daily-demand Parquet after Silver files exist:

```bash
DATA_LAKE_ROOT=data-lake \
cargo run --manifest-path src/RustDataPlatform/Cargo.toml -p lakehouse --bin build_gold
```

Expected output shape:

```text
wrote <n> daily demand rows to data-lake/gold/daily_demand/part-0000.parquet
```

Governance audit lives in PostgreSQL `governancedb`, schema `governance`. Useful demo tables:

```sql
SELECT proposal_id, sku_id, location_code, quantity, status, updated_at
FROM governance.reorder_proposals
ORDER BY updated_at DESC LIMIT 5;

SELECT outcome, reason_codes, policy_version, decided_at
FROM governance.policy_decisions
ORDER BY decided_at DESC LIMIT 5;

SELECT status, message, attempted_at
FROM governance.execution_attempts
ORDER BY attempted_at DESC LIMIT 5;
```

PostgreSQL correctness checks must show `0 <= reserved <= on_hand <= max_stock`, `safety_stock <= reorder_point <= max_stock`, no duplicate movement IDs, and source/warehouse movement counts equal after settling.

## Checkpoint log

1. Infrastructure smoke: Kafka, Connect, ClickHouse, PostgreSQL, Inventory, and Rust healthy.
2. CDC proof: one Inventory RESTOCK produced movement offset 3150 and balance offset 404; Rust accepted the real envelope without new dead letters.
3. Profile checkpoint: generator project tests passed 15/15.
4. Controlled run: `RegionalSpike`, 600 offered/600 HTTP 200, exactly 10.00 requests/sec for 60.00 seconds, no timeouts/drops/duplicate request IDs, all NCR.
5. SKU 42/NCR: 61 finalized Sale units entered the 5m/15m/1h windows; spike detection activated; adaptive forecast reached about 9,826/day; risk became CRITICAL; recommendation reached 97 units.
6. AUTO_DEMO: Inventory moved from on-hand 1 to its max 111 through idempotent RESTOCK API calls. A later recovery projection showed on-hand/available 111 and no incoming supply; all analytical open rows settled to zero.
7. Settled correctness: PostgreSQL and ClickHouse both held 3,722 distinct movement facts, 404 current balances, zero Inventory invariant violations, and zero source duplicate movement IDs.

The live spike deliberately exceeded SKU 42/NCR's physical `max_stock=111`, so its truthful risk remained CRITICAL even after capacity was full; one replenishment cannot cover a ~9,826-unit/day burst. The policy test separately proves risk falls from CRITICAL to LOW when the accepted restock provides sufficient forecast coverage. This is capacity-limit evidence, not a false green demo.

## Final verification record

- Affected AppHost dependency graph build: succeeded, zero warnings/errors; it builds Inventory, Ordering, Catalog, WebApp, generator, and hosting projects.
- Relevant .NET suites: 178 distinct tests passed after rerunning all 30 Inventory tests against isolated `inventory_cdc_tests`; zero relevant failures/skips.
- Earlier Rust checkpoint: `cargo fmt --all --check`, the then-current 18 workspace tests, and Clippy with `-D warnings` all passed.
- Live connector/task, Kafka, ClickHouse, Rust health/API, exact topic records, replay/dedupe, restart recovery, workload, and Inventory RESTOCK were verified.
- Repository-wide `dotnet test eShop.slnx --no-build` also exposed an unrelated environment gap: `ClientApp.UnitTests` had no built executable, and building it requires the uninstalled `maui-tizen` workload. No ClientApp files were changed; the nine runnable test assemblies passed in that broad attempt.

## Follow-up verification after lakehouse/governance extensions

The Rust data platform was extended with durable Bronze/Silver/Gold lakehouse storage, PostgreSQL governance audit tables, day-by-day forecast horizons, simulation alternatives, policy-gated proposals, and executor audit records.

Latest verified commands:

```bash
cd src/RustDataPlatform
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Result: formatting passed, 35 Rust workspace tests passed, and Clippy passed with warnings denied.

The currently running local Rust service reported healthy and had written 406 Bronze and 406 Silver records with zero dead letters. The Gold builder also ran successfully:

```bash
DATA_LAKE_ROOT=data-lake cargo run --manifest-path src/RustDataPlatform/Cargo.toml -p lakehouse --bin build_gold
```

Result: it wrote `0` daily-demand rows to `data-lake/gold/daily_demand/part-0000.parquet`, because the currently available Silver files did not contain `Sale` movement rows for the Gold daily-demand aggregate.

A final restart onto consumer group `eshop-rust-cdc-v3` could not be performed in this turn because the necessary outside-sandbox AppHost/Aspire execution was rejected by the current Codex usage limit.

## Known demo limitations

This is a local supplier simulator, not procurement. It models one open replenishment per SKU/location and a fixed lead-time delay. The dashboard is JSON, ClickHouse inserts are not yet batched, and event-time lateness uses an eight-day retention window rather than production watermarks.


## Continuous workload generator for live reorder-agent demos

The demo now has a continuous order workload mode. The target average is `600 orders/min` (`10 orders/sec`) and the pace intentionally rises/falls in a two-minute wave between roughly `300` and `900` orders/min.

Recommended Aspire configuration:

```bash
OrderGenerator__Enabled=true \
OrderGenerator__RatePerSecond=10 \
OrderGenerator__Continuous=true \
OrderGenerator__VariableRate=true \
OrderGenerator__WaveAmplitudeFraction=0.5 \
OrderGenerator__WavePeriodSeconds=120 \
OrderGenerator__MinimumRatePerSecond=5 \
OrderGenerator__MaximumRatePerSecond=15 \
aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj
```

For a stronger stockout/reorder-risk signal, add:

```bash
OrderGenerator__Profile=REGIONAL_SPIKE \
OrderGenerator__TargetSkuId=42
```

Expected flow:

```text
continuous random orders
    -> Ordering.API
    -> Inventory.API reservations/sales
    -> PostgreSQL inventory tables
    -> Debezium/Kafka
    -> Rust CDC/feature engine
    -> reorder-risk/proposal behavior
```
