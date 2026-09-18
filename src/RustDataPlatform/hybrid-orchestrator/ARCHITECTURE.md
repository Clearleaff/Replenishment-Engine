# Hybrid Orchestrator Architecture

This document explains the hybrid orchestrator from a system-design point of view. It focuses on boundaries, runtime flow, safety gates, failure behavior, and what must change before the service can execute real restocks.

## 1. System context

```mermaid
flowchart LR
    Ordering[Ordering / eShop event producer]
    Rabbit[(RabbitMQ queue)]
    Orchestrator[hybrid-orchestrator]
    Feature[feature-engine crate]
    Agent[replenishment-agent crate]
    Boss[Boss Desk HTTP API]
    Human[Human operator]
    Inventory[Inventory.API]
    Governance[(Future governance DB)]

    Ordering -->|Order stock event + balance snapshot| Rabbit
    Rabbit -->|AMQP delivery| Orchestrator
    Orchestrator --> Feature
    Orchestrator --> Agent
    Orchestrator -->|pending proposals| Boss
    Human -->|approve / inspect| Boss
    Boss -. LOG_ONLY command preview .-> Human
    Orchestrator -. future durable audit .-> Governance
    Boss -. future idempotent command .-> Inventory

    classDef future stroke-dasharray: 5 5,color:#777;
    class Governance,Inventory future;
```

Key point: Inventory remains the only service allowed to own stock. The current orchestrator only prepares proposals and command previews.

## 2. Internal module architecture

```mermaid
flowchart TB
    Main[src/main.rs]
    State[src/state.rs]
    Handler[src/event_handler.rs]
    Axum[Axum routes]
    Lapin[lapin consumer]
    Metrics[OrchestratorMetrics]
    Proposals[ProposalRecord store]
    Idempotency[seen event keys]

    Main -->|loads env| State
    Main -->|starts HTTP task| Axum
    Main -->|starts AMQP task| Handler
    Axum -->|read/update| State
    Handler -->|read config / write proposals| State
    Handler --> Lapin
    State --> Metrics
    State --> Proposals
    State --> Idempotency
```

The important refactor is that shared state is no longer a naked `Arc<Mutex<HashMap>>`. It is a typed `AppState` with named responsibilities.

## 3. Event-processing flow

```mermaid
flowchart TD
    A[RabbitMQ delivery] --> B{JSON parses?}
    B -- no --> B1[NACK requeue=false + invalidEvents]
    B -- yes --> C{payload valid?}
    C -- no --> C1[processing error + NACK requeue=false]
    C -- yes --> D{source event key seen?}
    D -- yes --> D1[ACK duplicate + duplicateEvents]
    D -- no --> E{balance snapshot present?}
    E -- no --> E1[ACK safe skip + featureSkips]
    E -- yes --> F{balance invariants hold?}
    F -- no --> F1[NACK requeue=false]
    F -- yes --> G[SkuLocationState apply snapshot + Sale movement]
    G --> H[calculate features]
    H --> I[ReplenishmentAgent evaluate]
    I --> J[SimulationEngine compare candidates]
    J --> K[LLM reasoner explain or fallback]
    K --> L[PolicyGate decide]
    L --> M{outcome}
    M -- AutoApproved --> M1[Approved proposal record + LOG_ONLY preflight]
    M -- RequiresHumanApproval --> M2[Proposed record for Boss Desk]
    M -- Rejected --> M3[Rejected proposal record]
    M1 --> Z[ACK]
    M2 --> Z
    M3 --> Z
```

Why this matters: bad data is rejected, missing stock state is not invented, duplicate deliveries are absorbed, and execution never happens behind the human's back.

## 4. Human-in-the-loop sequence

```mermaid
sequenceDiagram
    participant O as Ordering / Producer
    participant R as RabbitMQ
    participant H as hybrid-orchestrator
    participant F as feature-engine
    participant A as replenishment-agent
    participant U as Human operator

    O->>R: publish order stock event + balance snapshot
    R->>H: deliver message
    H->>H: validate payload + idempotency check
    H->>F: apply balance + sale movement, calculate features
    F-->>H: SkuLocationFeatures
    H->>A: evaluate decision + simulate alternatives
    A-->>H: decision, simulation, policy-ready proposal
    H->>A: PolicyGate decide
    A-->>H: AutoApproved / RequiresHumanApproval / Rejected
    H->>H: store ProposalRecord in memory
    H-->>R: ACK message
    U->>H: GET /api/v1/proposals
    H-->>U: proposals with decision/simulation/policy details
    U->>H: POST /api/v1/proposals/{id}/approve
    H-->>U: APPROVED + LOG_ONLY commandPreview
```

The LLM is not shown as an actor with system access because it does not get system access. It only receives deterministic decision/simulation context and returns explanation text.

## 5. Proposal state machine

```mermaid
stateDiagram-v2
    [*] --> Proposed: policy requires HITL
    [*] --> Approved: auto-approved in LOG_ONLY preflight
    [*] --> Rejected: policy rejected
    Proposed --> Approved: human approval endpoint
    Proposed --> Rejected: future reject endpoint
    Approved --> Executing: future real executor only
    Executing --> Succeeded: future Inventory success
    Executing --> Failed: future Inventory failure
```

Current implementation reaches `Proposed`, `Approved`, or `Rejected`. `Executing`, `Succeeded`, and `Failed` are future real-executor states.

## 6. Data contracts

### Incoming event

```json
{
  "eventId": "d94bc7e4-1e8a-4a3e-a596-c633e0a17eb1",
  "orderId": 1001,
  "skuId": 42,
  "locationCode": "NCR",
  "quantityDepleted": 8,
  "occurredAt": "2026-09-17T08:30:00Z",
  "balance": {
    "onHand": 12,
    "reserved": 0,
    "safetyStock": 10,
    "reorderPoint": 30,
    "maxStock": 100,
    "version": 9,
    "updatedAt": "2026-09-17T08:30:01Z",
    "authoritative": true
  }
}
```

### Stored proposal record

A `ProposalRecord` combines:

- proposal identity and status
- deterministic reorder decision
- simulation alternatives
- policy decision
- balance snapshot used for the decision
- data freshness status
- source event key
- execution attempts list
- human-readable message

This makes the approval API explainable. A user can see why a proposal exists before approving it.

## 7. Safety boundaries

| Boundary | Current behavior |
|---|---|
| Inventory state | Must arrive as balance snapshot; never guessed |
| LLM | Explanation only; no tools, DB, HTTP, shell, or queue access |
| Policy | Deterministic `PolicyGate` owns approval outcome |
| Execution | `LOG_ONLY`; no Inventory mutation |
| Idempotency | In-process event-key set; durable inbox still required |
| Approval storage | In-memory proposal store; durable governance DB still required |
| Bad messages | malformed JSON and invariant failures are nacked with `requeue=false` |

## 8. Failure and recovery model

```mermaid
flowchart LR
    Fail[Failure] --> Parse[Malformed JSON]
    Fail --> Invariant[Invalid balance invariant]
    Fail --> Duplicate[Duplicate delivery]
    Fail --> Missing[Missing balance]
    Fail --> Restart[Service restart]

    Parse --> ParseAction[NACK no requeue]
    Invariant --> InvAction[NACK no requeue]
    Duplicate --> DupAction[ACK skip]
    Missing --> MissingAction[ACK safe skip]
    Restart --> RestartAction[Pending in-memory proposals lost]
```

Production hardening priority: replace in-memory proposal/idempotency state with durable governance tables before any real execution is enabled.

## 9. Production-readiness checklist

Before enabling real stock mutation:

- [ ] durable proposal table
- [ ] durable incoming-event inbox table
- [ ] durable execution-attempt table
- [ ] DLQ topology and replay tooling
- [ ] authenticated approval endpoint
- [ ] authorized roles for approve/reject
- [ ] real Inventory command publisher or Inventory API client
- [ ] idempotent operation ID persisted before execution
- [ ] OpenTelemetry metrics/traces
- [ ] integration test with RabbitMQ + Inventory.API
- [ ] Aspire registration and health checks
- [ ] security review of event payload and approval API


## 10. Production hardening architecture

```mermaid
flowchart TB
    Rabbit[(RabbitMQ / Aspire eventbus)]
    Consumer[Resilient AMQP consumer]
    State[AppState feature store + proposal store]
    Feature[SkuLocationState rolling features]
    Agent[ReplenishmentAgent]
    Sim[SimulationEngine]
    LLM[Live LLM client Groq/OpenAI]
    Breaker[Circuit breaker + timeout]
    Limits[Hybrid hard limits]
    Policy[PolicyGate]
    Boss[Boss Desk API]

    Rabbit --> Consumer
    Consumer -->|event + balance snapshot| State
    State --> Feature
    Feature --> Agent
    Agent --> Sim
    Sim --> LLM
    LLM --> Breaker
    Breaker -->|valid structured JSON| Limits
    Breaker -->|429/timeout/5xx/malformed| Agent
    Limits -->|accepted| Policy
    Limits -->|rejected| Agent
    Policy --> State
    Boss --> State
```

The LLM is deliberately boxed in:

1. It receives only deterministic decision/simulation/balance context.
2. It must return `propose_action` JSON matching the schema.
3. Its HTTP call has a 3.5s timeout.
4. Rate limits/timeouts/errors open the circuit breaker and use deterministic fallback.
5. Hybrid hard limits reject rogue quantities/markdowns.
6. `PolicyGate` still decides whether a proposal can be auto-approved, rejected, or routed to HITL.

## 11. Message acknowledgement model

```mermaid
flowchart TD
    A[AMQP delivery] --> B{Parse JSON?}
    B -- no --> P[NACK requeue=false]
    B -- yes --> C{Validate schema and invariants?}
    C -- no --> P
    C -- yes --> D{Duplicate event?}
    D -- yes --> ACK[ACK]
    D -- no --> E[Update rolling feature state]
    E --> F{Transient platform failure?}
    F -- yes --> R[NACK requeue=true]
    F -- no --> G[LLM/governance/proposal]
    G --> ACK
```

Poison pills do not loop forever. Transient platform failures are requeued.

## 12. Runtime state

`AppState` now contains:

- typed config loaded from environment
- `LlmClient` with provider/circuit state
- in-process idempotency keys
- rolling `SkuLocationState` feature store per SKU/location
- proposal records with TTL
- audit events
- health/readiness fields
- operational metrics

The in-memory proposal/idempotency stores are production-shaped but not production-durable yet. Before real execution mode is enabled, move them to governance PostgreSQL tables.
