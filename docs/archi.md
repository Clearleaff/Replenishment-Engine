# Adaptive Inventory Replenishment Architecture

## System Architecture

Inventory PostgreSQL is the operational source of truth for stock. Kafka is event transport. The Bronze data lake is durable raw stream history. Silver and Gold Parquet are curated offline analytical datasets. ClickHouse is the fast analytical/read-model store. Governance PostgreSQL stores proposal and execution audit records. The agent explains and proposes; deterministic Rust policy and the executor control mutations.

```mermaid
flowchart TD
    WebApp --> Basket
    Basket --> Ordering
    Ordering --> InventoryAPI[Inventory.API]
    InventoryAPI --> InventoryDB[(PostgreSQL inventorydb)]
    InventoryDB --> Debezium
    Debezium --> Kafka
    Kafka --> RustConsumer[Rust CDC Consumer]
    RustConsumer --> Bronze[(Bronze Parquet data lake)]
    RustConsumer --> Silver[(Silver inventory events)]
    Silver --> Gold[(Gold demand datasets)]
    RustConsumer --> FeatureEngine[Real-time Feature Engine]
    FeatureEngine --> ClickHouse[(ClickHouse)]
    FeatureEngine --> Agent[Replenishment Agent]
    Agent --> Governance[(PostgreSQL governancedb)]
    Agent --> Executor
    Executor --> InventoryAPI
```

## PostgreSQL Logical Replication

The local AppHost configures PostgreSQL with `wal_level=logical`, `max_replication_slots=10`, and `max_wal_senders=10` when `DataPlatform:Enabled=true`. Debezium uses the stable replication slot `eshop_inventory_cdc` and publication `eshop_inventory_publication`.

## Kafka Topics

Current topics consumed by the Rust data platform:

- `eshop.inventory.inventory_movements`
- `eshop.inventory.inventory_balances`

Future order/payment topics can use the same Bronze identity and partition layout.

## Kafka To Bronze Data Lake Flow

```mermaid
flowchart LR
    KafkaTopic[Kafka topic partition offset] --> RawEvent[RawKafkaEvent]
    RawEvent --> Identity[topic + partition + offset]
    Identity --> Path[data-lake/bronze/kafka/topic/date/hour/part-partition-offset.parquet]
    Path --> Decode[Debezium decode]
    Decode --> Silver[SilverInventoryEvent]
```

Kafka offsets are committed only after Bronze persistence and downstream processing succeed. Replays write the same deterministic path and are counted as duplicates instead of creating uncontrolled duplicate files.

## Real-Time Feature Flow

```mermaid
flowchart LR
    SilverEvent[Normalized event] --> MovementDedup[Movement ID dedupe]
    SilverEvent --> BalanceVersion[Balance version gate]
    MovementDedup --> FeatureState[SKU/location feature state]
    BalanceVersion --> FeatureState
    FeatureState --> Windows[5m/15m/1h event-time windows]
    FeatureState --> Forecast[EWMA weekday/hour forecast]
    Forecast --> ClickHouseFeatures[(ClickHouse features)]
```

The feature engine keeps Reserve pressure separate from finalized Sale demand. Historical weekday/hour learning is updated from finalized event-time buckets, so repeated calculations do not overweight the same observation.

## Agent Decision Flow

```mermaid
flowchart TD
    Read[READ structured tools/context] --> Reason[REASON deterministic inputs]
    Reason --> Simulate[SIMULATE reorder alternatives]
    Simulate --> Propose[PROPOSE reorder]
    Propose --> Governance[GOVERNANCE audit]
    Governance --> Policy[POLICY gate]
    Policy --> Execute[EXECUTE through Inventory API]
```

The LLM boundary is purpose-built. It receives deterministic decision and simulation outputs and returns validated structured reasoning. It does not receive generic SQL, database access, filesystem access, shell access, arbitrary HTTP, or direct Inventory write access.

## Governance State Machine

```mermaid
stateDiagram-v2
    [*] --> PROPOSED
    PROPOSED --> REJECTED
    PROPOSED --> APPROVED
    APPROVED --> EXECUTING
    EXECUTING --> SUCCEEDED
    EXECUTING --> FAILED
```

Governance records live in `governancedb` under the `governance` schema:

- `agent_evaluations`
- `reorder_simulations`
- `reorder_proposals`
- `policy_decisions`
- `execution_attempts`

## Complete Closed-Loop Demo

```mermaid
sequenceDiagram
    participant Orders as Order Generator
    participant Inventory as Inventory.API
    participant Pg as inventorydb
    participant Debezium
    participant Kafka
    participant Rust as Rust Consumer
    participant Lake as Bronze/Silver/Gold
    participant CH as ClickHouse
    participant Gov as governancedb

    Orders->>Inventory: order lifecycle reserves/sells stock
    Inventory->>Pg: movement and balance rows
    Pg->>Debezium: logical WAL
    Debezium->>Kafka: inventory CDC topics
    Kafka->>Rust: consume record
    Rust->>Lake: persist raw Bronze and clean Silver
    Rust->>CH: features and analytical records
    Rust->>Gov: proposal, policy, execution audit
    Rust->>Inventory: executor restock call when policy allows
    Inventory->>Pg: restock movement and new balance
    Pg->>Debezium: restock CDC
    Debezium->>Kafka: restock event
    Kafka->>Rust: feature state updates
```

## Observability And Failure Handling

The Rust dashboard exposes `/health`, `/api/sku-locations`, and `/api/sku-locations/{sku}/{location}`. Health counters include consumed Kafka events, Bronze writes/duplicates, Silver writes/duplicates, normalization count, dead letters, duplicate movements, stale balances, decisions, simulations, proposals, policy decisions, executions, execution failures, LLM failures, and completed restocks.

Failure handling:

- Malformed CDC is stored in Bronze first, then written to dead letters.
- Duplicate Kafka delivery is idempotent by `topic + partition + offset`.
- Duplicate movements are idempotent by `movement_id`.
- Stale balances are rejected by Inventory balance version.
- Stale/unreconciled tool data blocks automatic execution.
- Inventory API conflicts are audited and do not mutate analytics into a false success.
- Production LLM provider fails closed until configured.

## Security Boundaries

Secrets are read from environment/configuration and are not committed. The agent has no raw database or shell access. Only the executor path can call Inventory API mutation endpoints, and it validates policy, identity, current inventory key, capacity, quantity, and idempotent operation ID first.
