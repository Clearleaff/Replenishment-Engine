# Inventory CDC Contract

## 1. Purpose and boundary

This contract is the boundary between the operational Inventory service and the analytical data platform. It was derived from the accepted EF Core model and migrations; it does not change Inventory code or its database ownership.

The initial Debezium capture allow-list contains exactly:

- `inventory.inventory_movements` — immutable business facts, normally insert-only.
- `inventory.inventory_balances` — the latest operational state, normally updated after reserve, sale, release, return, adjustment, or restock activity.

Inbox, integration-event log, reservations, locations, and shadow-check tables are deliberately excluded. Analytics must consume CDC rather than repeatedly query operational PostgreSQL after bootstrap/recovery.

## 2. Logical business identity

All downstream state is grouped by the UTF-8 key:

```text
<sku_id>:<location_code>
```

Example: `42:NCR`.

This is the future logical stream key used by features, model state, and reorder decisions. The original Debezium Kafka record key is still preserved during ingestion:

- movement topic key: `movement_id`
- balance topic key: composite `(sku_id, location_code)`

The Rust normalizer derives `sku_location_key` without changing source identity.

## 3. Source schema: inventory movements

Source: `src/Inventory.API/Infrastructure/Migrations/20260910070345_InitialInventory.cs`

| Column | PostgreSQL type | Null | Meaning |
|---|---|---:|---|
| `movement_id` | `uuid` | no | Primary key and primary fact-deduplication identity. |
| `source_event_id` | `uuid` | no | RabbitMQ/REST operation identity that caused the business change. |
| `sku_id` | `integer` | no | Catalog product ID used as SKU. |
| `location_code` | `varchar(16)` | no | Distribution center code, normalized uppercase. |
| `order_id` | `integer` | yes | Ordering aggregate ID when the movement belongs to an order. |
| `movement_type` | `varchar(24)` | no | `Reserve`, `Release`, `Sale`, `Restock`, `Return`, or `Adjustment`. |
| `quantity` | `integer` | no | Changed units. Non-adjustments are positive; adjustments may be signed but cannot be zero. |
| `occurred_at` | `timestamptz` | no | Business event time used for analytical windows. |
| `recorded_at` | `timestamptz` | no | Time Inventory persisted the fact. |
| `balance_version_after` | `bigint` | no | Version of the corresponding balance after this mutation. |
| `reason` | `varchar(200)` | yes | Optional human/machine explanation. |

Primary key: `PK_inventory_movements (movement_id)`.

Unique business constraint: `(source_event_id, sku_id, location_code, movement_type)`. This prevents a repeated source operation from applying the same movement type twice for one SKU/location.

Foreign key: `(sku_id, location_code)` references `inventory.inventory_balances`; deletion is restricted.

Important indexes support order lookup, recorded-time recovery, and event-time reads by SKU/location. Check constraints require non-zero quantity and a positive resulting balance version.

## 4. Source schema: inventory balances

| Column | PostgreSQL type | Null | Meaning |
|---|---|---:|---|
| `sku_id` | `integer` | no | Catalog product ID used as SKU. |
| `location_code` | `varchar(16)` | no | Distribution center code. |
| `on_hand` | `integer` | no | Physical units at the location. |
| `reserved` | `integer` | no | Units promised to orders but not yet sold/released. |
| `safety_stock` | `integer` | no | Operational safety threshold. |
| `reorder_point` | `integer` | no | Existing static operational reorder threshold. |
| `max_stock` | `integer` | no | Location/SKU capacity and restock cap. |
| `version` | `bigint` | no | Monotonic optimistic-concurrency version. |
| `updated_at` | `timestamptz` | no | UTC time of the last accepted state change. |

Composite primary key: `PK_inventory_balances (sku_id, location_code)`.

`available` is not a stored column. Consumers calculate it as `on_hand - reserved`.

Database constraints preserve:

```text
0 <= reserved <= on_hand <= max_stock
0 <= safety_stock <= reorder_point <= max_stock
available = on_hand - reserved
```

The Rust state store accepts a balance only when `incoming.version > stored.version`. Equal versions are duplicates and lower versions are stale/out-of-order; both are ignored.

## 5. Kafka topics and operation semantics

The connector uses topic prefix `eshop` and retains Debezium's default `<prefix>.<schema>.<table>` naming:

```text
eshop.inventory.inventory_movements
eshop.inventory.inventory_balances
```

Debezium operations are interpreted as follows:

| `op` | Meaning | Movement handling | Balance handling |
|---|---|---|---|
| `c` | create/insert | Normalize `after` as a fact and deduplicate by `movement_id`. | Accept `after` if its version is newer than stored state. |
| `u` | update | Quarantine because movements are expected to be immutable; do not silently rewrite history. | Normalize `after`; accept only a strictly newer version. |
| `d` | delete | Record a source anomaly/tombstone marker; do not subtract history. | Remove/mark state only if an explicit future deletion policy allows it; MVP quarantines it. |
| `r` | snapshot/read | Normalize like an insert and deduplicate. | Bootstrap state using the same monotonic-version rule. |

A Kafka null-value tombstone following `d` is acknowledged but contains no business row to normalize. Unknown fields are ignored for forward compatibility. Malformed required fields go to structured error logging and are not committed as successful business events.

## 6. Expected Debezium records

Kafka Connect is configured with JSON converters and schemas disabled. These examples show the meaningful shape; connector/source metadata may gain extra fields in compatible Debezium releases.

During the first local bootstrap the worker emitted already-buffered records in Kafka Connect's wrapped form, `{ "schema": {...}, "payload": {...} }`. The decoder intentionally accepts both that form and the schema-less form below. New connector records use schema-less JSON because the converter settings are also pinned in the connector configuration. Supporting both shapes makes a converter rollout/replay safe without leaking either transport representation into feature code.

### Movement insert (`op=c`)

Kafka key:

```json
{"movement_id":"d8adbe37-c24e-4b51-b7c7-50d98e45ada1"}
```

Kafka value:

```json
{
  "before": null,
  "after": {
    "movement_id": "d8adbe37-c24e-4b51-b7c7-50d98e45ada1",
    "source_event_id": "bd288d46-2f80-470a-85f4-7b65c4825388",
    "sku_id": 42,
    "location_code": "NCR",
    "order_id": 9012,
    "movement_type": "Sale",
    "quantity": 2,
    "occurred_at": "2026-09-10T10:11:45.123456Z",
    "recorded_at": "2026-09-10T10:11:45.130000Z",
    "balance_version_after": 18,
    "reason": null
  },
  "source": {
    "connector": "postgresql",
    "name": "eshop",
    "db": "inventorydb",
    "schema": "inventory",
    "table": "inventory_movements",
    "lsn": 123456789
  },
  "op": "c",
  "ts_ms": 1789035105130
}
```

### Balance update (`op=u`)

Kafka key:

```json
{"sku_id":42,"location_code":"NCR"}
```

Kafka value:

```json
{
  "before": {
    "sku_id": 42,
    "location_code": "NCR",
    "on_hand": 100,
    "reserved": 2,
    "safety_stock": 20,
    "reorder_point": 40,
    "max_stock": 200,
    "version": 17,
    "updated_at": "2026-09-10T10:11:44.900000Z"
  },
  "after": {
    "sku_id": 42,
    "location_code": "NCR",
    "on_hand": 98,
    "reserved": 0,
    "safety_stock": 20,
    "reorder_point": 40,
    "max_stock": 200,
    "version": 18,
    "updated_at": "2026-09-10T10:11:45.123456Z"
  },
  "source": {
    "connector": "postgresql",
    "name": "eshop",
    "db": "inventorydb",
    "schema": "inventory",
    "table": "inventory_balances",
    "lsn": 123456789
  },
  "op": "u",
  "ts_ms": 1789035105131
}
```

Snapshot events use the same row shape in `after` with `op=r`. Delete records place the removed row in `before`, set `after` to null, and can be followed by a null Kafka tombstone.

## 7. Ordering, idempotency, and time

- Kafka delivery and Debezium recovery are treated as at-least-once.
- `movement_id` is the authoritative movement dedupe key; `source_event_id` is retained for tracing and additional anomaly checks.
- Balance state never moves backward because versions must increase strictly.
- `occurred_at` is event time for sales/reservation windows. `recorded_at`, connector `ts_ms`, and consumer arrival time are processing/ingestion observations only.
- A balance update and its movement fact originate in one Inventory PostgreSQL transaction, but consumers must not assume they arrive on two topics at the same instant.
- `Sale`, not `Reserve`, is finalized historical demand. Reserve activity is measured separately as short-term pressure.

## 8. Failure and recovery rules

- A missing required identity, invalid UUID/timestamp, or impossible quantity is rejected by normalization and logged with topic/partition/offset.
- A repeated movement is ignored after the first successful warehouse insert.
- An older/equal balance version is ignored and counted as stale/duplicate.
- Unknown operation types are not guessed; they are logged and skipped.
- Connector downtime leaves committed changes in WAL until the replication slot resumes. WAL retention must therefore be monitored.
- Snapshot replay is safe because both fact identity and balance version handling are idempotent.

## 9. Phase A verification

Read-only inspection covered the C# entities, EF configurations, initial migration, reliability migration, and model snapshot. It confirmed that only the two allow-listed tables are required for the initial analytical stream, and that no Inventory baseline edit or new migration is necessary for CDC source validation.

The live smoke test later issued an idempotent one-unit RESTOCK for `42:NCR`. Kafka movement offset `3150` contained `op=c`, `source_event_id=5d6133fb-f1a7-47c6-939b-223432f57e34`, and resulting version `2`; balance offset `404` contained `op=u`, `on_hand=62`, and version `2`. This proves the two records originated from the Inventory transaction and reached their deliberately named topics.
