# SYSTEM ENGINEERING DEEP DIVE: `hybrid-orchestrator`

**File Target:** `src/RustDataPlatform/hybrid-orchestrator/SYSTEM_ENGINEERING_DEEP_DIVE.md`
**Role:** Principal Systems Architect & Core Rust Contributor.
**Audience:** VP of Engineering, Senior Architect, and Audit Compliance Committee.

This dossier serves as an exhaustive, line-by-line, module-by-module, mathematical, and architectural technical specification for the `hybrid-orchestrator` crate. This document systematically unpacks the low-level technical execution, memory layout, operational invariants, data serving mechanics, and failure modes across the service.

---

## Section 1: Architectural Rationale & Threat Modeling

The `hybrid-orchestrator` acts as the critical operational brain of the eShop inventory replenishment platform. Traditional agentic systems allow LLMs (Large Language Models) unconstrained execution freedom, leading to stochastic, non-deterministic, and sometimes catastrophic failures (hallucinations resulting in rogue orders).

To counteract this, the `hybrid-orchestrator` implements a **Hybrid Pattern** comprising:
1. **Deterministic Math:** High-speed, statistically sound feature computation (`feature-engine`) and discrete simulation.
2. **Sandboxed LLM:** An "air-gapped" reasoning engine restricted to analysis.
3. **PolicyGate:** A hardcoded, rule-based governance boundary that vetoes non-compliant outputs.

### Threat Model & Security Boundaries
The core threat vector is prompt injection or LLM drift leading to invalid capacity utilization (e.g., ordering 1,000,000 units instead of 100).
- **Air-Gapped LLM Strategy:** The `LlmClient` holds **zero I/O permissions**. It has no SQL database credentials, no filesystem descriptors, and no network sockets aside from a tightly configured, timeout-bound HTTPS client pointing at the inference API.
- **Strict JSON Enforcement:** The LLM's output is forcibly confined to the `propose_action` JSON schema (enforcing types, minimums, maximums).
- **Preflight Veto Validation:** Even if the LLM produces valid JSON, `HybridPolicyLimits::validate_action` intercepts it and validates the recommendation against absolute deterministic bounds mathematically derived from real-time stock levels.

### Rust Memory Safety & Zero-Cost Abstractions
Rust is selected over garbage-collected runtimes (Python/C#) because the orchestrator processes unbounded AMQP data streams. A GC pause under high load could cause RabbitMQ channels to timeout or hit memory limits. Rust's ownership model and `Arc<Mutex<T>>` synchronization primitives prevent data races at compile time, guaranteeing that concurrent Axum webhooks and Tokio AMQP consumers never corrupt the `AppState`.

---

## Section 2: Crate Topology & Dependency Graph (`Cargo.toml`)

The dependency topology in `Cargo.toml` is engineered for high-performance, concurrent, and fault-tolerant streaming.

### Core Ecosystem Dependencies
- **`tokio` (`workspace = true`):** The engine driving the asynchronous execution context. It provides multi-threaded scheduling, I/O polling, `tokio::spawn`, and `tokio::select!`. A multi-threaded environment is crucial to ensure CPU-bound simulation math does not starve I/O-bound AMQP heartbeats.
- **`axum`:** A highly ergonomic, low-overhead HTTP routing framework built on `tower` and `hyper`. It is utilized in `src/main.rs` to serve the Human-in-the-Loop (HITL) Webhook and liveness telemetry.
- **`lapin` (`2.3.1`):** A robust AMQP client library providing connection multiplexing. Used as the RabbitMQ bridge to the upstream .NET eShop EventBus.
- **`tokio-retry` (`0.3`):** Wraps raw connection logic with an `ExponentialBackoff` strategy, resolving race conditions where the orchestrator boots before RabbitMQ is ready.
- **`reqwest`:** Used for executing OpenAI-compatible chat completions over HTTPS, with strict timeout controls.
- **`serde` & `serde_json`:** Facilitate hyper-fast zero-copy serialization/deserialization across network boundaries (RabbitMQ ingress and LLM egress).

### Internal Platform Crates
The orchestrator avoids reinventing the wheel by depending on production-grade platform components:
- **`data-platform-common`:** Injects shared data models like `InventoryBalanceState` and `InventoryMovementFact`.
- **`replenishment-agent`:** Encapsulates the heavy domain logic (`ReorderDecision`, `ReorderSimulation`, `ProposalStatus`, `PolicyGate`).
- **`feature-engine`:** Manages rolling features and sliding windows (`SkuLocationState`).

---

## Section 3: Configuration & Dynamic Topology Discovery (`src/config.rs`)

`config.rs` entirely eliminates hardcoded infrastructure parameters, providing robust parsing fallbacks via environment variables.

### The `OrchestratorConfig` Struct
```rust
pub struct OrchestratorConfig {
    pub amqp_url: String,
    pub queue_name: String,
    pub consumer_tag: String,
    pub bind_addr: SocketAddr,
    pub execution_mode: ExecutionMode,
    pub llm: LlmConfig,
    pub proposal_ttl: Duration,
    pub cleanup_interval: Duration,
    pub amqp_max_retry_delay: Duration,
}
```

### Dynamic Resolution Mechanics (`resolve_amqp_url`)
The system discovers the message broker dynamically using an explicit precedence order, critical for environments like Kubernetes or .NET Aspire:
1. `AMQP_URL` (Direct explicit override)
2. `ConnectionStrings__eventbus` / `ConnectionStrings__EventBus` (.NET Aspire config injection standard)
3. `amqp://127.0.0.1:5672/%2f` (Fallback to local dev server)

### `LlmConfig` and Deterministic Hard Caps
The LLM configuration automatically discovers its provider context:
- It checks `GROQ_API_KEY` $\rightarrow$ activates `LlmProvider::Groq` (default model `llama-3.3-70b-versatile`).
- It checks `OPENAI_API_KEY` $\rightarrow$ activates `LlmProvider::OpenAi` (default model `gpt-4o-mini`).
- If neither is present, it degrades gracefully to `LlmProvider::Disabled` (operating purely on deterministic mathematics).

**Safety Caps (Hard Limits):**
- `LLM_TIMEOUT_MILLISECONDS`: Defaults to `3500ms`. Prevents infinite connection blocking on the AMQP consumer loop if the external LLM provider stalls.
- `LLM_CIRCUIT_FAILURE_THRESHOLD`: Configured to `3` sequential failures before tripping the circuit breaker.
- `LLM_MAX_MARKDOWN_PERCENT`: Capped rigidly at `20.0`%.
- `LLM_MAX_ORDER_QUANTITY`: Capped rigidly at `5000` units per operation.

---

## Section 4: Concurrency & Shared State Architecture (`src/state.rs`)

The `AppState` is the centralized, thread-safe memory registry injected via Axum `State` extractors into web requests and cloned into Tokio background tasks.

### Struct Memory Layout
```rust
pub struct AppState {
    pub config: Arc<OrchestratorConfig>,
    pub llm: LlmClient,
    proposals: Arc<Mutex<HashMap<Uuid, ProposalRecord>>>,
    seen_events: Arc<Mutex<HashSet<String>>>,
    feature_states: Arc<Mutex<HashMap<SkuLocation, SkuLocationState>>>,
    metrics: Arc<Mutex<OrchestratorMetrics>>,
    amqp_connected: Arc<Mutex<bool>>,
}
```

### Locking Topology & Contention Mitigation
Notice that `AppState` relies on partitioned `Arc<Mutex<T>>` buckets rather than a monolithic global lock (`Arc<Mutex<AppState>>`). 
- **Reasoning:** A Webhook triggering `state.metrics()` will exclusively acquire the `metrics` lock, meaning it will **not block** the `amqp_task` attempting to mutate `seen_events` or `feature_states`. This topology inherently eliminates cross-domain deadlocks and minimizes lock contention under high concurrency.

### Proposal State Machines
Proposals advance through a finite state machine via the `ProposalStatus` enum:
- `Proposed` $\rightarrow$ Requires `approve()` (HITL) or resolves implicitly via `AutoApproved`.
- `Approved` $\rightarrow$ Transitioned successfully.
- `Rejected` $\rightarrow$ Blocked by `PolicyGate` rules.
- `Failed` $\rightarrow$ Transitioned internally when TTL elapses.

### The `approve()` State Transition
When the webhook calls `state.approve(proposal_id)`, it initiates a critical safety sequence:
1. Validates existence: Returns `ApproveError::NotFound` if missing.
2. Validates state: If status is not `Proposed`, returns `ApproveError::NotPending`.
3. Validates TTL: If `expires_at < Utc::now()`, the state mutates to `ProposalStatus::Failed`, an audit entry `APPROVAL_REJECTED_EXPIRED` is appended, and it returns `ApproveError::Expired`.
4. Executes mutation: Shifts status to `Approved`, appends a deterministic `AuditRecord`, and modifies `record.message`.

### In-Process Idempotency & Bounded Memory
Network retries guarantee *at-least-once* delivery. `remember_event(&self, key: &str)` prevents duplicate processing by inserting a deterministic `source_event_key` into an atomic `HashSet`.
To prevent OOM (Out-of-Memory) faults, `cleanup_expired()` runs synchronously in a background loop, purging entries from the `HashMap` where `expires_at >= now`. Memory is strictly bounded to the active time window.

---

## Section 5: Resilient AMQP Consumer & Message Semantics (`src/event_handler.rs`)

The `event_handler::start_consumer` bootstrapper manages infinite resiliency loops, ensuring the system recovers from broker disconnections without fatal panics.

### Connection Recovery via `tokio_retry`
```rust
let strategy = ExponentialBackoff::from_millis(500).factor(2).max_delay(state.config.amqp_max_retry_delay);
Retry::start(strategy, move || { ... })
```
If RabbitMQ restarts, `lapin::Connection::connect` will fail. Rather than crashing the OS process, the orchestrator backs off exponentially up to 30 seconds, attempting continuous healing.

### Queue Declaration & Consumer Limits
The orchestrator declares the queue (`eshop.inventory.order_stock_confirmed`) with `durable: true`, ensuring messages survive broker restarts.

### Message Processing & Strict Acknowledgment Semantics
The event loop retrieves deliveries (`consumer.next().await`) and enforces rigid transactional semantics:
1. **Parsing Check:** Converts binary data to `OrderStockConfirmedIntegrationEvent`. If malformed, issues `delivery.nack(multiple: false, requeue: false)` $\rightarrow$ routes message to Dead Letter Queue (DLQ).
2. **Idempotency Check:** Checks `state.remember_event()`. If duplicate, issues `delivery.ack()` and halts execution (returning `HandleOutcome::Duplicate`).
3. **Payload Invariants Check:** During `build_features`, calls `balance.invariants_hold()`. If $\text{Available} \neq \text{OnHand} - \text{Reserved}$, it raises `HandlerError::Poison` and NACKs without requeuing.
4. **Transient Infrastructure Failures:** If calculation or LLM network fails critically (raising `HandlerError::Transient`), the system executes `nack(requeue: true)`, allowing future consumers to retry.
5. **Success:** Issues `delivery.ack()`.

---

## Section 6: Feature Engine & Adaptive Statistical Forecasting

This phase mathematically synthesizes historical trends into forward-looking intelligence.

### Building `InventoryMovementFact`
In `build_features`, the payload is transformed into an `InventoryMovementFact` with `movement_type: InventoryMovementType::Sale` and the precise `quantity_depleted`. 
The orchestrator leverages `state.update_features()` to delegate statistical calculation to the `feature_states` cache.

### Rolling Window Aggregates & EWMA
`SkuLocationState::apply_movement()` ingests the fact, subsequently executing sliding windows:
- 5-minute, 15-minute, and 1-hour sales velocity variables (`sale_units_5m`, `sale_velocity_1h`, etc.).
- Computes `AdaptiveForecast` using Exponentially Weighted Moving Averages (EWMA) to model baseline demand (`baseline_units_per_day`) vs recent demand (`recent_units_per_day`).
- Computes a mathematical standard deviation and derives a `spike_score`. If `recent_vs_baseline_ratio` breaches the algorithm's tolerance, `spike_detected` flips to `true`, altering downstream simulation urgency.

---

## Section 7: Simulation Engine & Candidate Exploration

Rather than allowing an LLM to hallucinate arbitrary numerical constraints, the system executes a mathematical grid search over discrete operational scenarios.

### Candidate Generation Algorithm
The `candidate_quantities()` function strategically defines bounded candidates:
$$ \text{Capacity} = \max(0, \text{MaxStock} - \text{OnHand}) $$
$$ \text{Candidates} = \left\{0, \; \lfloor \tfrac{\text{Recommended}}{2} \rfloor, \; \text{Recommended}, \; \lceil 1.25 \times \text{Recommended} \rceil, \; \text{Capacity}\right\} $$
*Note: Candidates are sorted and deduplicated using `.sort_unstable()` and `.dedup()`.*

### Simulation Execution
These discrete candidates are mapped across `SimulationEngine::compare_reorder_quantities()`. The engine mathematically evaluates risk, stockout exposure, excess inventory penalties, and the exact `demand_coverage_ratio`. The outcomes are attached to the `ReorderSimulation` struct, providing the LLM and the PolicyGate with bulletproof ground-truth comparisons.

---

## Section 8: Sandboxed LLM Strategy Officer Client (`src/llm_client.rs`)

The `llm_client.rs` manages the non-deterministic heuristic layer. It enforces deterministic boundaries and shields the core system from provider degradation.

### The JSON Schema Protocol
The HTTP payload to Groq/OpenAI leverages native `reqwest` builders, injecting prompt instructions specifying structural limits:
```json
"response_format": {
    "type": "json_schema",
    "json_schema": { "name": "propose_action", "strict": true, "schema": { ... } }
}
```
The prompt specifically instructs: *"Do not request tools, databases, APIs, shell, or filesystem access."*

### Deterministic Firewall (`HybridPolicyLimits`)
When the `ChatCompletionResponse` payload is parsed by `parse_action_json`, it traverses the ultimate defense array in `validate_action`:
1. `if action.reorder_quantity < 0 { bail!(...) }`
2. `if action.reorder_quantity > max_order_quantity { bail!(...) }`
3. `if action.reorder_quantity > capacity { bail!(...) }`
4. `if action.markdown_percent > max_markdown_percent { bail!(...) }`
5. Confidence validations (`0.0..=1.0`).

If *any* boundary is breached, the execution generates a deterministic warning, blocks the proposal, and logs a failure on the circuit breaker.

### The Circuit Breaker State Machine
If the LLM provider experiences throttling (`HTTP 429 TOO_MANY_REQUESTS`), times out, returns HTTP 5xx, or hallucinates schema deviations, `record_failure()` increments `consecutive_failures`.
- **Tripping:** At $N=3$ (defined by `LLM_CIRCUIT_FAILURE_THRESHOLD`), the circuit sets `opened_at = Some(Instant::now())`.
- **Bypass Mode:** While open (`circuit_is_open()`), subsequent events bypass network IO instantly, returning `LlmFallbackReason::CircuitOpen` and using `deterministic_reasoning` generated via local math.
- **Cooldown:** After `circuit_cooldown` (30s) elapses, the circuit transitions to Half-Open and resets `consecutive_failures`.

---

## Section 9: Deterministic PolicyGate & Governance Boundaries

Regardless of LLM behavior, business governance holds absolute veto power.

### Policy Execution (`handle_event`)
The `PolicyGate::new().decide()` evaluator parses the `ReorderProposal`, `ReorderDecision`, and `DataFreshnessStatus`. It emits one of three `PolicyOutcome` branches:
- **`PolicyOutcome::AutoApproved`:** The operation is deemed low-risk. The orchestrator invokes `ProposalExecutor::validate_start(&proposal, &policy, &balance)`. Because `ExecutionMode::LogOnly` is enforced, it records an audit log indicating the simulated command emission, logging the `operation_id`. The event returns `HandleOutcome::AutoApprovedPrepared`.
- **`PolicyOutcome::RequiresHumanApproval`:** Anomalous operations (demand spikes, non-final data freshness) freeze the state, saving the `ProposalRecord` in the cache map. The webhook (HITL) must intervene.
- **`PolicyOutcome::Rejected`:** Explicit denial resulting in `ProposalStatus::Rejected`.

---

## Section 10: Human-in-the-Loop (HITL) Webhook & Graceful Shutdown (`src/main.rs`)

The HTTP Webhook layer is built on `axum`, acting as the API serving interface for dashboard integration and deployment monitoring.

### Axum Routing & HTTP Serving
The data is actively served using a RESTful pattern. Axum's `State` extractor binds `AppState` to the route handlers.
- **`GET /health`** $\rightarrow$ `health()`: Polls `state.health()`. Assesses `amqp_connected`, `metrics`, and LLM connectivity status. Serves HTTP 200 (`OK`) if ready, HTTP 503 (`SERVICE_UNAVAILABLE`) if AMQP is disconnected.
- **`GET /api/v1/proposals`** $\rightarrow$ `list_proposals()`: Returns a time-sorted JSON array of all active `ProposalRecord` cache entries.
- **`POST /api/v1/proposals/:proposal_id/approve`** $\rightarrow$ `approve_proposal()`: Serves as the webhook entry point. Intercepts the HTTP call, binds the `Uuid` from the Path, invokes `state.approve()`, and elegantly maps Rust Result Enums to standard HTTP Codes (`404 NOT_FOUND`, `409 CONFLICT`, `410 GONE`, `202 ACCEPTED`).

### The Tokio Concurrency Harness & OS Signal Traps
The `hybrid-orchestrator` does not exit violently. The `main` runtime coordinates four infinite asynchronous fibers via the `tokio::select!` macro:
1. `shutdown_signal()`: Traps OS-level signals (`SIGINT` via Ctrl+C, or `SIGTERM` issued by Kubernetes schedulers).
2. `http_task`: Runs `axum::serve(listener, app)`.
3. `amqp_task`: Runs `event_handler::start_consumer(consumer_state)`.
4. `cleanup_task`: Periodically ticks `time::interval` to call `cleanup_expired()`.

When a Kubernetes `SIGTERM` triggers `shutdown_signal()` to resolve, the `select!` macro cancels all pending branches. This causes the AMQP connection to drain naturally and in-flight HTTP requests to flush correctly, resulting in an enterprise-grade graceful termination protocol.

---

*This document was auto-generated to secure architectural alignment and provide deep-dive compliance analysis across the eShop `hybrid-orchestrator` crate.*
