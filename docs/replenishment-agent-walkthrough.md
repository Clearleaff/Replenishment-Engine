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

The required deterministic test trains Sunday at about 40 units/day and Tuesday at about 5. It asserts Sunday forecast is higher, injects a Tuesday spike, asserts detection activates, and asserts Tuesday adaptive forecast rises.

## `replenishment-agent/src/lib.rs`

`AgentMode` has three explicit values:

- `OBSERVE`: calculate/store decisions without action (default).
- `RECOMMEND`: produce an approval-required recommendation.
- `AUTO_DEMO`: execute only when the hosting environment is Development.

`ReplenishmentPolicyConfig` contains lead time, review period, service factor, and risk thresholds. `ReorderDecision` records inputs and outputs: identity/time, demand estimates, spike/risk/stockout ETA, inventory position, safety/target quantities, recommendation, reason codes, model version, and mode.

The calculation runs sequentially:

```text
inventory_position = on_hand - reserved + incoming
lead_time_demand   = forecast_per_day * lead_time_days
safety_stock       = max(operational safety,
                         service_factor * demand_stddev * sqrt(lead_time_days))
target_inventory   = lead_time_demand + review_period_demand + safety_stock
reorder_quantity   = clamp(target - inventory_position,
                           0, max_stock - on_hand - incoming)
stockout_eta       = available / adaptive_daily_rate
```

Risk thresholds classify ETA as LOW/MEDIUM/HIGH/CRITICAL. A stable UUIDv5 decision ID makes an identical evaluation auditable/idempotent. Tests cover low/high demand, earlier stockout response, max-stock capping, sufficient inventory, and stable identity.

Static `reorder_point` remains an operational guardrail; adaptive reorder uses learned demand, uncertainty, lead time, review time, open supply, and capacity. They answer different questions.

## `cdc-consumer/src/main.rs` closed loop

After a balance evaluation, every decision is inserted into ClickHouse and exposed by the dashboard. In `AUTO_DEMO`, only a positive recommendation with no existing open replenishment is accepted:

1. Persist an `OPEN` replenishment with operation ID equal to the stable decision ID.
2. Add its quantity to in-memory incoming supply so repeated evaluations do not reorder it.
3. Wait `SIMULATED_LEAD_TIME_SECONDS`.
4. POST to Inventory's existing `/api/inventory/restocks?api-version=1.0` endpoint.
5. Never write operational PostgreSQL directly.
6. Mark the simulated replenishment completed.
7. Observe the resulting movement/balance again through PostgreSQL -> Debezium -> Kafka.

Inventory's operation idempotency protects a retry/restart from applying the restock twice. Recovered `OPEN` rows are rescheduled on process restart.

The live spike exposed an important cross-topic ordering case: a RESTOCK movement can arrive before its newer balance. Re-evaluating that movement against the old low balance could schedule another order. The consumer therefore stores every movement fact but performs pre-balance recalculation only for Reserve/Sale demand signals; Restock/Release wait for the authoritative balance record. If Inventory returns `409` because stock is already at capacity, the simulator closes the analytical row as `NO_CAPACITY` and removes phantom incoming supply.

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
