# Rust CDC Data Pipeline Walkthrough

## Why Rust and where it sits

The Rust workspace is an asynchronous analytics consumer. It never owns operational stock and never writes Inventory tables. It consumes Kafka, normalizes transport envelopes, keeps per-SKU/location features, stores analytics in ClickHouse, and exposes a read-only dashboard API.

## Files, in implementation order

### `src/RustDataPlatform/Cargo.toml` and `Cargo.lock`

The workspace lists `common`, `feature-engine`, `replenishment-agent`, `warehouse`, and `cdc-consumer`. Shared dependency versions prevent crate drift. `Cargo.lock` fixes the exact resolved dependency graph for reproducible application builds. Stable Rust 1.97 and edition 2024 were used.

### Crate `Cargo.toml` files

Each crate declares only the dependencies it uses. `common` owns serde/time/UUID models; `feature-engine` depends on common; `lakehouse` owns local Bronze/Silver/Gold Parquet files; `replenishment-agent` depends on features; `warehouse` implements online analytical persistence; `cdc-consumer` composes them with Tokio, rdkafka, reqwest, tracing, and Axum. This direction prevents Kafka or ClickHouse types from entering business calculations.

### `lakehouse/src/lib.rs`

Responsibility: persist every consumed Kafka record before normalization and provide the first offline analytical transformations.

Sequential flow:

1. `RawKafkaEvent` stores topic, partition, offset, Kafka timestamp, key bytes, value bytes, headers, ingestion timestamp, and schema version.
2. Binary key/value/header data is base64 encoded so the exact transport bytes can be preserved inside a text-friendly Parquet schema.
3. `KafkaEventIdentity` is the deterministic identity: `topic:partition:offset`.
4. `LocalBronzeLake` writes one Parquet file per Kafka record under `data-lake/bronze/kafka/<topic>/date=YYYY-MM-DD/hour=HH/part-<partition>-<offset>.parquet`.
5. If that path already exists, persistence returns `AlreadyExists`; a retry/restart therefore does not create uncontrolled duplicates.
6. The writer creates a temporary `.parquet.tmp` file first and renames it into place only after Polars finishes writing. Kafka offsets should be committed only after this durable write and downstream processing succeed.
7. `SilverInventoryEvent` converts typed `NormalizedEvent` values into clean structured inventory events while retaining source topic/partition/offset.
8. `daily_sales_gold` groups Silver Sale movements into daily SKU/location demand rows for offline forecasting and manager-demo datasets.
9. `write_parquet` is shared by Bronze, Silver, and Gold outputs.
10. `build_gold` is a small batch command. It scans `data-lake/silver/inventory_events`, reads Silver Parquet files with Polars, groups Sale movements into daily SKU/location demand rows, and writes `data-lake/gold/daily_demand/part-0000.parquet`.

This crate deliberately owns Polars and Parquet dependencies. The hot feature engine still receives typed Rust structs; it does not need to know how files are laid out.

Checkpoint result:

```bash
cd src/RustDataPlatform
cargo test -p lakehouse
```

Result: 5 tests passed. The tests cover topic/date/hour partitioning, idempotent duplicate persistence, tombstone-shaped raw records, Silver-to-Gold daily sales aggregation, and reading back a written Parquet file with Polars.

Checkpoint result after adding Silver readback and the Gold batch command:

```bash
cd src/RustDataPlatform
cargo test -p lakehouse
```

Result: 7 tests passed. The new coverage reads Silver Parquet back into typed events and proves those events can produce the Gold daily-demand dataset.

### `common/src/model.rs`

Responsibility: normalized cross-crate vocabulary.

Sequentially, it defines:

- `SkuLocation`, which uppercases/trim-normalizes location and produces `sku:location` keys.
- Debezium operations and Inventory movement types.
- `InventoryMovementFact`, the immutable business fact.
- `InventoryBalanceState`, including calculated `available`, invariant validation, and key creation.
- `NormalizedEvent`, whose variants make snapshot/create/update/delete/tombstone behavior explicit.
- The two exact Kafka topic constants.

Everything downstream depends on these types; they depend on no Kafka envelope.

### `common/src/debezium.rs`

Responsibility: turn raw bytes into `NormalizedEvent` or a typed `DecodeError`.

Important flow:

1. A Kafka null value becomes `Tombstone`; no business effect is guessed.
2. JSON is parsed once. If a Kafka Connect `{schema,payload}` wrapper exists, the decoder unwraps `payload`; otherwise it uses the schema-less object.
3. `source.table` selects the exact typed row parser.
4. `c`/`r` movement rows become facts; movement `u` is quarantined because history is immutable; `d` becomes an explicit deletion event.
5. `c`/`u`/`r` balances calculate `available` and must satisfy Inventory invariants.
6. Serde ignores unknown fields but rejects missing/wrong required fields.

Tests cover real schema-less and schema-wrapped records, unknown fields, snapshots, malformed JSON, immutable movement updates, and tombstones.

### `feature-engine/src/lib.rs`

Responsibility: event-time state per SKU/location.

`DecayedStat` is an exponentially weighted online mean/variance. `OnlineDemandModel` maintains seven weekday buckets, 24 hour buckets, a recent EWMA, counts, and last update. `SkuLocationState` keeps recent Reserve and Sale queues separately, a movement-ID set, current balance, and model.

On each fact:

1. Reject an already-seen `movement_id`.
2. Insert by `occurred_at`, not arrival time, so a reasonable late event still lands in the right window.
3. Count `Sale` as finalized demand; keep `Reserve` only as pressure.
4. Prune retained events after eight days.
5. Calculate 5m/15m/1h units and per-day velocities.
6. Blend weekday/hour history with recent rates; a larger spike ratio increases recent weight.

On each balance, accept only a version strictly greater than stored. This makes replays and cross-topic reordering safe.

### `warehouse/src/lib.rs`

`AnalyticalWarehouse` is the replaceable sink boundary; `ClickHouseWarehouse` is its local implementation. HTTP SQL initialization creates:

- `fact_inventory_movements`
- `inventory_balance_current`
- `sku_location_minute_features`
- `sku_location_model_state`
- `reorder_decisions`
- `open_replenishments`
- `cdc_dead_letters`

ReplacingMergeTree versions make current balance/model/open-replenishment reads deterministic with `FINAL`. Movement identity is checked before insert. JSONEachRow inserts preserve typed timestamps/UUIDs. Recovery loads balance versions, serialized online models, and open simulated replenishments.

The first live run exposed two ClickHouse HTTP details. Query-only POST requires a real body length, so `execute` sends SQL as a UTF-8 body. ClickHouse's default JSON timestamp text has no timezone suffix, while Rust's `DateTime<Utc>` correctly requires one; recovery queries therefore request ISO timestamp output. Targeted warehouse/consumer tests passed before restart, and live restart validates the persisted-row path.

### `cdc-consumer/src/main.rs`

This executable wires every boundary:

1. Parse environment and refuse `AUTO_DEMO` outside Development.
2. Initialize ClickHouse and recover balance/model/open-replenishment state.
3. Rebuild a read-only dashboard projection from recovered balances/models, so a quiet Kafka stream does not make the dashboard blank after restart. This projection is not treated as a new persisted/actionable decision.
4. Start Axum on `/health`, `/api/sku-locations`, and the SKU/location detail route.
5. Create an rdkafka consumer with manual commits, `earliest` recovery, and `read_committed` isolation.
6. Build a raw Kafka event from topic, partition, offset, timestamp, key, payload, and headers.
7. Persist that raw event to the Bronze data lake before attempting Debezium decoding.
8. Decode each message; malformed records are already preserved in Bronze, then recorded in the dead-letter table.
9. Persist successfully decoded records to the Silver data lake before feature processing.
10. Deduplicate movement facts and reject stale balance versions.
11. Persist facts, features, model state, and audited decisions.
12. Commit Kafka offset only after raw persistence and the processing path succeed.

`DATA_LAKE_ROOT` controls the local root and defaults to `data-lake`. AppHost passes the repository-local `data-lake` path unless `DataPlatform:DataLakeRoot` overrides it. The new health counters are `bronze_written`, `bronze_duplicates`, `silver_written`, and `silver_duplicates`.

Consumer group `eshop-rust-cdc-v3` intentionally replays retained topics after the snapshot-reset compatibility fix. Stable movement identity, Bronze/Silver path identity, and balance versions make replay safe. Snapshot balance records are allowed to reset recovered local demo state, while normal update records still reject stale versions. A future DLQ reprocessor can reuse the same decoder without changing feature logic.

Checkpoint result after the Bronze/Silver integration:

```bash
cd src/RustDataPlatform
cargo test -p lakehouse -p cdc-consumer
```

Result: 9 tests passed. This covers consumer route/recalculation behavior plus lakehouse duplicate/replay-safe persistence.

### Snapshot replay fix

During live local replay, the consumer recovered old ClickHouse balance state from an earlier demo run, then saw fresh Debezium snapshot records from a reset local `inventorydb`. Those snapshot records had lower balance versions, so the normal optimistic-version guard counted them as stale. That was correct for ordinary updates, but too strict for a new Debezium snapshot.

Changed files:

- `src/RustDataPlatform/feature-engine/src/lib.rs`
- `src/RustDataPlatform/cdc-consumer/src/main.rs`
- `src/eShop.AppHost/DataPlatformExtensions.cs`

Meaningful code change:

```rust
pub fn apply_balance_snapshot(&mut self, state: InventoryBalanceState) {
    self.balance = Some(state);
}
```

Line-by-line:

- `pub fn apply_balance_snapshot(...)` creates a separate path for Debezium snapshot records.
- `&mut self` means this function updates the in-memory SKU/location state.
- `state: InventoryBalanceState` is the authoritative inventory balance decoded from Kafka.
- `self.balance = Some(state);` stores the snapshot even if its version is lower than a previously recovered local demo value.

In `cdc-consumer`, the balance handler now checks `operation == ChangeOperation::Snapshot`. Snapshot records call `apply_balance_snapshot`; ordinary create/update records still call `apply_balance` and keep rejecting stale versions. This keeps production optimistic-concurrency behavior while allowing safe local snapshot recovery.

`DataPlatformExtensions.cs` also moves the local consumer group to `eshop-rust-cdc-v3` so retained Kafka data is replayed through the corrected code path during the next Aspire start.

Verification:

```bash
cd src/RustDataPlatform
cargo test -p feature-engine -p cdc-consumer -p governance -p lakehouse -p replenishment-agent -p warehouse
```

Result: 30 tests passed. The new regression test is `snapshot_balance_can_reset_recovered_local_demo_state`.

### `.gitignore`

The repository now ignores `src/RustDataPlatform/target/` and `data-lake/`. Rust build artifacts and local demo lake files are generated outputs, not source. Durable local data remains on disk for the demo, but it is intentionally not committed.

### `src/eShop.AppHost/DataPlatformExtensions.cs`

The Rust data platform executable now receives `DATA_LAKE_ROOT`. By default, this points at the repository-local `data-lake` folder. That keeps local demos simple while preserving a clean boundary for replacing the path with object storage later.

The executable also receives `ConnectionStrings__governancedb` through Aspire's `.WithReference(governanceDb)` wiring. The Rust process uses that only through the purpose-built `GovernanceStore` API; the LLM never receives this connection string.

### `src/eShop.AppHost/Program.cs`

`governancedb` is registered on the existing PostgreSQL server alongside `inventorydb`. This preserves the storage split:

- `inventorydb`: operational inventory source of truth.
- `governancedb`: proposal, policy, and execution audit.
- ClickHouse: fast analytical query/read-model store.

Checkpoint result:

```bash
dotnet build src/eShop.AppHost/eShop.AppHost.csproj --no-restore -v:minimal
dotnet test tests/eShop.AppHost.UnitTests/eShop.AppHost.UnitTests.csproj --no-restore -v:minimal
```

Result: AppHost build succeeded with 0 warnings and 0 errors. AppHost unit tests passed 17/17. The same build failed silently inside the restricted sandbox, but passed when run outside the sandbox with the approved `dotnet build` command path.

## Time and delivery concepts

- Event time: `occurred_at`, when Inventory says the movement happened; window membership uses this.
- Processing time: when Rust happens to receive/process it; used for operational observation, not demand truth.
- Window: a bounded interval ending at evaluation time, such as the preceding 15 minutes.
- At least once: Kafka/Connect may redeliver after failure; movement IDs and balance versions prevent duplicate business analytics.
- Consumer group: Kafka stores one progress position per topic/partition for all instances sharing the group.
- Partition/key: the balance source key is SKU/location and the movement source key is movement ID. The MVP uses one partition per topic, while downstream state always derives `sku:location`.

## Verification

```bash
cd src/RustDataPlatform
cargo fmt --all --check
cargo test --workspace
curl http://127.0.0.1:8088/health
curl http://127.0.0.1:8088/api/sku-locations/42/NCR
```

Latest local verification:

```bash
cd src/RustDataPlatform
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Rust formatting passed.
- Rust workspace tests passed: 35 tests.
- Clippy passed with warnings denied.
- `git diff --check` passed.

Live local process health before restart:

```bash
curl -sS http://127.0.0.1:8088/health
```

Result: the already-running process was healthy and had written 406 Bronze and 406 Silver records with zero dead letters. Because the final AppHost/Aspire restart requires outside-sandbox execution and that approval was rejected by the current Codex usage limit, the post-`eshop-rust-cdc-v3` live replay was not rerun in this turn.

## Known MVP limitations

- One consumer process and in-memory hot feature state; ClickHouse persists recovery state.
- Eight-day in-memory event retention and no watermark/allowed-lateness side output yet.
- Per-record HTTP inserts favor clarity over batching throughput.
- A crash after ClickHouse write but before Kafka commit may replay; identities/versions absorb it.
- Deleting source facts/state is treated as an anomaly rather than silently reversing history.
