# Rust CDC Data Pipeline Walkthrough

## Why Rust and where it sits

The Rust workspace is an asynchronous analytics consumer. It never owns operational stock and never writes Inventory tables. It consumes Kafka, normalizes transport envelopes, keeps per-SKU/location features, stores analytics in ClickHouse, and exposes a read-only dashboard API.

## Files, in implementation order

### `src/RustDataPlatform/Cargo.toml` and `Cargo.lock`

The workspace lists `common`, `feature-engine`, `replenishment-agent`, `warehouse`, and `cdc-consumer`. Shared dependency versions prevent crate drift. `Cargo.lock` fixes the exact resolved dependency graph for reproducible application builds. Stable Rust 1.97 and edition 2024 were used.

### Crate `Cargo.toml` files

Each crate declares only the dependencies it uses. `common` owns serde/time/UUID models; `feature-engine` depends on common; `replenishment-agent` depends on features; `warehouse` implements persistence; `cdc-consumer` composes them with Tokio, rdkafka, reqwest, tracing, and Axum. This direction prevents Kafka or ClickHouse types from entering business calculations.

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
6. Decode each message; malformed records are persisted in the dead-letter table.
7. Deduplicate movement facts and reject stale balance versions.
8. Persist facts, features, model state, and audited decisions.
9. Commit Kafka offset only after the processing path succeeds.

Consumer group `eshop-rust-cdc-v2` intentionally replayed retained topics after the real-envelope compatibility fix. Stable movement identity and balance versions make that replay safe. A future DLQ reprocessor can reuse the same decoder without changing feature logic.

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

The workspace checkpoint passed 18 tests. Live replay normalized records with zero new dead letters and populated per-SKU/location dashboard state.

## Known MVP limitations

- One consumer process and in-memory hot feature state; ClickHouse persists recovery state.
- Eight-day in-memory event retention and no watermark/allowed-lateness side output yet.
- Per-record HTTP inserts favor clarity over batching throughput.
- A crash after ClickHouse write but before Kafka commit may replay; identities/versions absorb it.
- Deleting source facts/state is treated as an anomaly rather than silently reversing history.
