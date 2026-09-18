# Hybrid Orchestrator Walkthrough

This document explains the hybrid orchestrator as it exists after hardening. It is written to be honest: this crate is now a safer bridge, but it is not yet the authoritative restock executor.

## Mental model

```text
RabbitMQ event
    ↓
validate payload
    ↓
require balance snapshot
    ↓
feature-engine calculates demand/forecast features
    ↓
replenishment-agent creates deterministic reorder decision
    ↓
simulation engine compares candidate quantities
    ↓
LLM reasoner explains only; it does not decide
    ↓
PolicyGate decides auto / human / reject
    ↓
proposal is stored in memory
    ↓
HTTP Boss Desk can inspect and approve
    ↓
LOG_ONLY command preview is returned
```


## Architecture diagram

```mermaid
flowchart TB
    subgraph External[External systems]
        Producer[Ordering / event producer]
        Rabbit[(RabbitMQ)]
        Human[Human operator]
    end

    subgraph Hybrid[hybrid-orchestrator]
        Main[main.rs]
        Handler[event_handler.rs]
        State[state.rs / AppState]
        Api[Axum Boss Desk API]
    end

    subgraph Platform[Existing RustDataPlatform crates]
        Feature[feature-engine]
        Agent[replenishment-agent]
        Policy[PolicyGate]
        Sim[SimulationEngine]
    end

    Producer --> Rabbit
    Rabbit --> Handler
    Main --> Handler
    Main --> Api
    Handler --> State
    Api --> State
    Handler --> Feature
    Handler --> Agent
    Agent --> Sim
    Agent --> Policy
    Human --> Api
```

## Runtime flow diagram

```mermaid
flowchart TD
    A[AMQP delivery] --> B[Deserialize JSON]
    B --> C[Validate order/SKU/location/quantity]
    C --> D[Build source event key]
    D --> E{Duplicate?}
    E -- yes --> E1[ACK and skip]
    E -- no --> F{Balance present?}
    F -- no --> F1[ACK safe skip: no fabricated inventory]
    F -- yes --> G[Validate inventory invariants]
    G --> H[Apply balance snapshot]
    H --> I[Apply deterministic Sale movement]
    I --> J[SkuLocationState::calculate]
    J --> K[ReplenishmentAgent::evaluate]
    K --> L[SimulationEngine::compare_reorder_quantities]
    L --> M[LlmReasoner explanation or mock fallback]
    M --> N[PolicyGate::decide]
    N --> O{Policy outcome}
    O -- AutoApproved --> O1[Approved proposal, LOG_ONLY preflight]
    O -- RequiresHumanApproval --> O2[Store pending proposal]
    O -- Rejected --> O3[Store rejected proposal]
    O1 --> P[ACK]
    O2 --> P
    O3 --> P
```

For the longer sequence/state diagrams, see [ARCHITECTURE.md](ARCHITECTURE.md).

## What changed from the Gemini draft

### Removed unsafe stock fabrication

Old behavior:

```rust
let on_hand_after = (50_i32 - event.quantity_depleted).max(0);
```

That was not production-safe. It made up inventory state from an arbitrary number.

New behavior:

- The event must include a `balance` snapshot.
- If no balance exists, the event is skipped and counted.
- If balance invariants fail, processing fails safely.

### Added typed application state

`state.rs` now owns:

- `OrchestratorConfig`
- `ExecutionMode`
- `ProposalRecord`
- `OrchestratorMetrics`
- `AppState`

This is cleaner than passing a naked `Arc<Mutex<HashMap<Uuid, ReorderProposal>>>` everywhere.

### Added explicit execution mode

Only `LOG_ONLY` exists today. That is intentional.

Approval now means:

> Human accepted this proposal, and the service prepared a command preview.

It does not mean:

> Stock was restocked.

That honesty matters because Inventory.API remains the sole stock owner.

### Added idempotency

Each event gets a source key:

- use `eventId` if present
- otherwise use `orderId:skuId:locationCode:quantityDepleted`

The process keeps an in-memory `HashSet` of source keys. Duplicate deliveries are acked and skipped.

This is good enough for local/dev, but production still needs durable inbox storage.

### Added health and review API

Routes:

```text
GET  /health
GET  /api/v1/proposals
GET  /api/v1/proposals/{proposal_id}
POST /api/v1/proposals/{proposal_id}/approve
```

These endpoints make the bridge inspectable instead of relying on logs only.

## File-by-file explanation

### `Cargo.toml`

Added:

```toml
tracing-subscriber.workspace = true
thiserror.workspace = true
```

`tracing-subscriber` is required because `main.rs` initializes structured logging. `thiserror` is available for future typed errors; the crate currently uses `anyhow` for bridge-level error propagation.

### `src/main.rs`

Responsibilities:

1. initialize logging
2. load config from environment
3. build shared `AppState`
4. start Axum HTTP API
5. start RabbitMQ consumer task
6. shut down on Ctrl-C

Important details:

- `/health` returns current metrics.
- proposal routes expose the in-memory proposal records.
- approval is conflict-safe: approving an already-approved/rejected proposal returns `409`.
- approval returns a command preview, not a fake execution success.

### `src/state.rs`

Responsibilities:

- centralize runtime config
- hold proposal records
- hold in-process duplicate event keys
- hold metrics
- implement proposal approval state transition

Important structs:

- `OrchestratorConfig`: environment-derived settings.
- `ExecutionMode`: currently only `LOG_ONLY`.
- `ProposalRecord`: audit object joining proposal, decision, simulation, policy, balance, and status.
- `OrchestratorMetrics`: operational counters.
- `AppState`: cloned into Axum and AMQP tasks.

### `src/event_handler.rs`

Responsibilities:

1. connect to RabbitMQ
2. declare/consume the configured queue
3. parse and validate events
4. deduplicate events
5. calculate features using `SkuLocationState`
6. evaluate with `ReplenishmentAgent`
7. simulate with `SimulationEngine`
8. explain with LLM fallback
9. govern with `PolicyGate`
10. store proposal record

Important safety rules:

- malformed JSON is nacked with `requeue=false`
- invalid balance invariants stop processing
- missing balance does not create fake features
- non-authoritative balance becomes `UNRECONCILED`
- `UNRECONCILED` data requires human approval through the existing policy gate
- LLM failure falls back to deterministic explanation

## Runtime flow in plain English

1. RabbitMQ delivers an event saying an order consumed inventory.
2. The orchestrator checks that the event has real SKU, location, quantity, and balance data.
3. It converts the balance into `InventoryBalanceState`.
4. It converts the order consumption into a deterministic Sale movement fact.
5. The feature engine computes rolling sales windows and adaptive forecast values.
6. The replenishment agent decides whether more stock is needed.
7. The simulator tests several quantities against capacity.
8. The LLM writes an explanation, but cannot approve or execute anything.
9. The policy gate decides whether this is auto-safe, human-required, or rejected.
10. The proposal record is stored in memory for the HTTP API.
11. A human can inspect and approve it.
12. Approval returns a command preview only.

## Verification performed

```bash
cd src/RustDataPlatform
cargo fmt --all --check
cargo test -p hybrid-orchestrator
cargo clippy -p hybrid-orchestrator --all-targets -- -D warnings
```

Result:

- formatting passed
- 4 focused tests passed
- Clippy passed with warnings denied

## Remaining work before production execution

This bridge is much better now, but these are still required before it can mutate stock:

1. Persist proposals and inbox entries in PostgreSQL governance tables.
2. Add authentication and authorization to approval endpoints.
3. Add a real command publisher or Inventory.API executor client.
4. Persist execution attempts before calling Inventory.
5. Add retry/DLQ topology and operational alarms.
6. Add integration tests using real RabbitMQ payloads from Ordering/Inventory.
7. Add OpenTelemetry metrics/traces.
8. Wire through Aspire only after the queue contract is finalized.


## Production hardening changes

### `src/config.rs`

This file centralizes environment configuration. The service now reads RabbitMQ from `AMQP_URL` or Aspire's `ConnectionStrings__eventbus`, so it no longer depends on a hardcoded local port. It also selects the LLM provider:

1. Groq when `GROQ_API_KEY` exists.
2. OpenAI when `OPENAI_API_KEY` exists.
3. deterministic fallback when neither exists.

### `src/llm_client.rs`

This file is the live Strategy Officer client. It sends OpenAI-compatible chat-completions requests to Groq/OpenAI with strict JSON schema output. It returns a typed `LlmActionProposal` and validates basic shape before the policy layer sees it.

Failure behavior is intentionally conservative:

- HTTP 429, timeout, HTTP 5xx, malformed JSON, or circuit-open state falls back to deterministic reasoning.
- The AMQP consumer is not blocked indefinitely because the client timeout defaults to 3.5 seconds.
- Hybrid hard limits reject rogue LLM outputs such as excessive reorder quantity or markdown.

### `src/event_handler.rs`

The handler now keeps rolling feature state in `AppState` instead of creating a fresh `SkuLocationState` for every event. That means 5m/15m/1h velocity can accumulate across events in the process.

The AMQP connection now uses exponential backoff and reconnects after disconnects. Acknowledgement behavior is explicit:

- `ack` on success/duplicate.
- `nack(requeue=false)` for poison pills.
- `nack(requeue=true)` for transient platform failures.

### `src/state.rs`

State now includes proposal TTL cleanup, audit records, AMQP readiness, LLM status, rolling feature states, and metrics for LLM fallback/policy rejection/reconnects.

### Required verification

```bash
cd src/RustDataPlatform
cargo test -p hybrid-orchestrator
cargo clippy -p hybrid-orchestrator --all-targets -- -D warnings
```

Current result after hardening: 8 tests passed, including:

- LLM JSON schema parser test.
- Circuit breaker fallback test when provider is unavailable.
- Policy boundary rejection test for exaggerated LLM order quantity.
