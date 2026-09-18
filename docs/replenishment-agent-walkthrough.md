# Adaptive Replenishment Agent Walkthrough

## Goal

For each `SKU + distribution center`, learn normal seasonal demand, react to recent abnormal demand, estimate stockout risk, and produce an auditable recommendation. Numerical forecasting is deterministic Rust code, not an LLM.

## `feature-engine/src/lib.rs`

The online model updates incrementally, so it does not retrain a batch model for every event.

- EWMA gives newer observations more weight while old behavior decays gradually.
- Seven weekday statistics learn that Sunday and Tuesday can differ.
- Twenty-four hour statistics capture intra-day patterns.
- Running variance measures uncertainty, not just the mean.
- The recent signal blends 5m, 15m, and 1h Sale velocity.
- `recent_vs_baseline_ratio` and `spike_score` increase when recent demand is unusually high.
- Adaptive forecast trusts seasonality during calm periods and recent velocity during spikes.

Reserve is excluded from learned sales demand; it remains a separate pressure feature. Model JSON is stored in ClickHouse and restored at startup.

The model now learns closed historical buckets instead of cumulative metrics on every calculation:

1. Sale movements increment event-time daily and hourly totals.
2. `calculate()` still computes live 5m/15m/1h windows for current features.
3. A prior day is observed into the weekday model only once, after that day is closed.
4. A prior hour is observed into the hourly model only once, after that hour is closed.
5. Recent-rate EWMA is sampled at most once per minute.

This prevents repeated dashboard refreshes or balance replays from overweighting the same historical demand.

The required deterministic test trains Sunday at about 40 units/day and Tuesday at about 5. It asserts Sunday forecast is higher, injects a Tuesday spike, asserts detection activates, and asserts Tuesday adaptive forecast rises.

## `replenishment-agent/src/lib.rs`

`AgentMode` has three explicit values:

- `OBSERVE`: calculate/store decisions without action (default).
- `RECOMMEND`: produce an approval-required recommendation.
- `AUTO_DEMO`: execute only when the hosting environment is Development.

`ReplenishmentPolicyConfig` contains lead time, review period, service factor, and risk thresholds. `ReorderDecision` records inputs and outputs: identity/time, demand estimates, spike/risk/stockout ETA, inventory position, safety/target quantities, horizon demand, day-by-day horizon forecast, recommendation, reason codes, model version, and mode.

The calculation runs sequentially:

```text
inventory_position = on_hand - reserved + incoming
lead_time_demand   = forecast_per_day * lead_time_days
safety_stock       = max(operational safety,
                         service_factor * demand_stddev * sqrt(lead_time_days))
target_inventory   = sum(day_by_day_forecast_horizon) + safety_stock
reorder_quantity   = clamp(target - inventory_position,
                           0, max_stock - on_hand - incoming)
stockout_eta       = available / adaptive_daily_rate
```

Risk thresholds classify ETA as LOW/MEDIUM/HIGH/CRITICAL. A stable UUIDv5 decision ID makes an identical evaluation auditable/idempotent. Tests cover low/high demand, earlier stockout response, max-stock capping, sufficient inventory, and stable identity.

## Forecast horizon, simulation, LLM boundary, and governance

The deterministic path is now:

```text
READ -> REASON -> SIMULATE -> PROPOSE -> GOVERNANCE -> EXECUTE
```

`OnlineDemandModel::forecast_horizon` produces separate daily forecast points. If today is Friday and the horizon is three days, the decision records Friday, Saturday, and Sunday separately. This lets Sunday historical demand increase the target honestly.

`SimulationEngine::compare_reorder_quantities` evaluates fixed candidate quantities such as 20, 81, and 150. For every option it calculates projected inventory position, expected stockout units, excess units, coverage ratio, service score, risk, and max-stock capacity violation. It then selects the lowest-stockout option, breaking ties by lower excess.

`LlmReasoner` is a bounded explanation interface. `MockLlmReasoner` works without a key and only explains deterministic Rust outputs. `ProductionLlmReasoner` is a placeholder boundary and fails closed until a real provider is configured. `parse_llm_reasoning_json` validates structured output and rejects malformed, empty, or out-of-range responses.

`ReorderProposal`, `PolicyGate`, `PolicyDecision`, and `ProposalExecutor` model the governance path. The policy code, not the LLM, decides whether a proposal can execute. Stale or unreconciled data prevents automatic execution. HIGH and CRITICAL risk require approval unless an explicit deterministic demo policy enables automatic execution.

## `governance/src/lib.rs`

Governance has its own PostgreSQL boundary. Inventory PostgreSQL remains the operational stock source of truth, while `governancedb` stores the audit trail for agent evaluations, simulations, proposals, policy decisions, and execution attempts.

Sequentially:

1. `GovernanceStore` defines purpose-built write methods. It does not expose arbitrary SQL to the agent or LLM.
2. `PostgresGovernanceStore::connect` opens the configured connection string injected by Aspire as `ConnectionStrings__governancedb`.
3. `initialize()` creates the `governance` schema and five tables idempotently.
4. Evaluation, simulation, policy, and execution inserts use stable primary keys with `ON CONFLICT DO NOTHING`.
5. Proposal writes upsert state because proposal status can move through the lifecycle.
6. JSONB columns hold structured simulation alternatives and LLM reasoning without giving the LLM raw database access.

Static `reorder_point` remains an operational guardrail; adaptive reorder uses learned demand, uncertainty, lead time, review time, open supply, and capacity. They answer different questions.

## `cdc-consumer/src/main.rs` closed loop

After a balance evaluation, the runtime persists the deterministic decision, simulation alternatives, proposal, policy decision, and executor validation attempt into both ClickHouse analytical tables and `governancedb` audit tables. The dashboard exposes the latest decision/proposal/policy status for each SKU/location. In `AUTO_DEMO`, only a positive proposal with deterministic auto-policy approval and no existing open replenishment is accepted:

1. Persist an `OPEN` replenishment with operation ID equal to the stable proposal ID.
2. Add its quantity to in-memory incoming supply so repeated evaluations do not reorder it.
3. Wait `SIMULATED_LEAD_TIME_SECONDS`.
4. POST to Inventory's existing `/api/inventory/restocks?api-version=1.0` endpoint.
5. Never write operational PostgreSQL directly.
6. Persist execution result.
7. Mark the simulated replenishment completed.
8. Observe the resulting movement/balance again through PostgreSQL -> Debezium -> Kafka.

Inventory's operation idempotency protects a retry/restart from applying the restock twice. Recovered `OPEN` rows are rescheduled on process restart.

The live spike exposed an important cross-topic ordering case: a RESTOCK movement can arrive before its newer balance. Re-evaluating that movement against the old low balance could schedule another order. The consumer therefore stores every movement fact but performs pre-balance recalculation only for Reserve/Sale demand signals; Restock/Release wait for the authoritative balance record. If Inventory returns `409` because stock is already at capacity, the simulator closes the analytical row as `NO_CAPACITY` and removes phantom incoming supply.

`AUTO_DEMO` uses an explicit deterministic demo policy that can auto-approve HIGH/CRITICAL proposals in Development only. Normal `OBSERVE` and `RECOMMEND` modes keep HIGH/CRITICAL proposals approval-required.

## Dashboard fields

`GET /api/sku-locations/{sku}/{location}` exposes on-hand/reserved/available, 5m/15m/1h sales, baseline/recent/adaptive demand, spike score, risk, stockout time, last recommendation, and incoming supply. `/health` exposes processing and restock counters.

## Failure modes

- No history: forecast starts conservatively at recent demand; operational safety stock still applies.
- Zero forecast: stockout ETA is absent rather than division by zero.
- Duplicate fact/stale balance: ignored and counted.
- Supplier simulation HTTP failure: the open row stays recoverable; Inventory is unchanged.
- AUTO_DEMO in non-development: startup fails closed.
- Capacity limit: recommendation is capped; it cannot request inventory beyond `max_stock`.

## Verification checkpoint

Feature/model/policy/recovery tests passed as part of the 18-test Rust workspace. This includes a deterministic check that sufficient restock coverage moves the same 40-units/day forecast from CRITICAL to LOW risk. Live OBSERVE replay populated audited decisions without issuing restocks. AUTO_DEMO verification and its before/after evidence are recorded in [adaptive-replenishment-demo.md](adaptive-replenishment-demo.md).

Additional checkpoint after the finalized-bucket fix:

```bash
cd src/RustDataPlatform
cargo test -p feature-engine -p replenishment-agent
```

Result: 10 tests passed. The new regression calls `calculate()` twice over the same closed sale bucket and verifies the model observation count does not increase a second time.

Additional checkpoint after horizon/simulation/governance additions:

```bash
cd src/RustDataPlatform
cargo test -p feature-engine -p replenishment-agent
```

Result: 16 tests passed. Coverage includes day-by-day Sunday horizon demand, deterministic simulation alternatives, malformed LLM output rejection, stale-data policy blocking, and executor preflight validation.

Additional checkpoint after governance runtime integration:

```bash
cd src/RustDataPlatform
cargo test -p replenishment-agent -p warehouse -p cdc-consumer
```

Result: 14 tests passed. This verifies the agent domain, ClickHouse schema presence for governance tables, and consumer integration tests after routing the AUTO_DEMO path through proposal/policy/executor validation.

Additional checkpoint after adding the dedicated Governance PostgreSQL store:

```bash
cd src/RustDataPlatform
cargo test -p governance -p replenishment-agent -p warehouse -p cdc-consumer
```

Result: 15 tests passed. This verifies the governance schema, agent domain, ClickHouse audit schema, and consumer wiring after adding `governancedb`.

Additional checkpoint after the Debezium snapshot replay fix:

```bash
cd src/RustDataPlatform
cargo test -p feature-engine -p cdc-consumer -p governance -p lakehouse -p replenishment-agent -p warehouse
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Result: focused tests passed 30/30, full Rust workspace tests passed 35/35, and Clippy passed with warnings denied.

The important agent behavior is that recovered analytical state can now be replaced by a fresh Debezium snapshot after a local database reset. Only snapshot records get that privilege. Ordinary balance updates still use version ordering so stale updates cannot roll the model backward.
