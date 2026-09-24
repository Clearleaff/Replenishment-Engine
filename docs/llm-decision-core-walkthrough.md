# Walkthrough: LLM-Driven Decision Core Refactoring
# Walkthrough: LLM-Driven Decision Core & Multi-Provider Architecture

## Overview

We refactored the `hybrid-orchestrator` crate in `RustDataPlatform` to replace its deterministic decision layer with an **LLM-driven decision core** utilizing Groq tool-calling.
We refactored the `hybrid-orchestrator` crate in `RustDataPlatform` to replace its static evaluation layer with an **LLM-driven decision core** supporting multi-turn tool calling across **Groq** (`qwen/qwen3.8-27b`), **Google Gemini** (`gemini-3.1-flash-lite`), and **OpenAI**.
We refactored the `hybrid-orchestrator` crate in `RustDataPlatform` to replace its static evaluation layer with an **LLM-driven decision core** supporting multi-turn tool calling across **Groq** (`openai/gpt-oss-20b`), **Google Gemini** (`gemini-3.1-flash-lite`), and **OpenAI**.

Deterministic arithmetic (statistical EWMA, seasonal decomposition, sample stddev, linear trend slope, grid-search simulation) remains in pure Rust as **tools that the LLM calls**. No business thresholds or formula multipliers are hardcoded in the system prompt or Rust code.
Deterministic arithmetic (statistical EWMA, seasonal decomposition, sample standard deviation, linear trend slope, grid-search simulation) remains in pure Rust as **tools that the LLM calls**. No business thresholds or formula multipliers are hardcoded in the system prompt or Rust code.

---

## 1. What Was Changed & Added

```
src/RustDataPlatform/
├── .env.example                     [NEW] Safe environment configuration template (gitignored .env)
├── .env.example                     [NEW] Multi-provider environment configuration template
├── warehouse/
│   ├── src/lib.rs                   [EXTENDED] Added event_calendar & supplier_disruption_signals DDLs, queries, and analytical warehouse methods
│   └── seeds.sql                    [NEW] DDL and seed records (Indian festivals 2026-2027 & synthetic supplier disruption signals)
├── replenishment-agent/
│   └── src/lib.rs                   [EXTENDED] Added ToolCallRecord trace struct and updated LlmReasoning with tools_used and tool_calls
├── hybrid-orchestrator/
│   ├── Cargo.toml                   [MODIFIED] Added warehouse crate dependency
│   ├── ARCHITECTURE.md              [UPDATED] Comprehensive architecture document covering tool calling, firewall, and trace architecture
│   ├── src/config.rs                [EXTENDED] System prompt extracted to config; added max_identical_tool_calls, ClickHouseConfig, max_tool_rounds, tool_call_timeout, automatic .env loader
│   ├── src/safety_firewall.rs       [NEW] Thin deterministic safety gate running AFTER LLM decision
│   ├── src/llm_tools.rs             [NEW] 8 Tool definitions, loop guard with configurable threshold, and Rust-executed arithmetic/simulation
│   ├── src/llm_client.rs            [REWRITTEN] Multi-turn Groq tool loop, retry-after backoff, 1-retry on parse error, deterministic fallback
│   ├── src/state.rs                 [EXTENDED] Added ClickHouseWarehouse and get_feature_state to AppState
│   └── src/event_handler.rs         [REWIRED] Swapped evaluate() call with propose_with_tools(); preserved idempotency filter
└── send_spike.py                    [FIXED] Dynamic SKU argument parsing
│   ├── ARCHITECTURE.md              [UPDATED] Comprehensive architecture document covering tool calling, firewall, and governance
│   ├── README.md                    [UPDATED] Runtime endpoints, configuration, and architecture summary
│   ├── RUNBOOK.md                   [UPDATED] Operating instructions, health checks, quota management, and testing
│   ├── SYSTEM_ENGINEERING_DEEP_DIVE.md [UPDATED] Low-level systems specification and memory topology
│   ├── src/config.rs                [EXTENDED] Multi-provider enum, model aliasing, mandatory domain tool system prompt, and automatic .env loader
│   ├── src/safety_firewall.rs       [NEW] Thin deterministic safety gate with capacity clamping and stockout risk elevation
│   ├── src/llm_tools.rs             [NEW] 8 Compacted tool definitions (808 prompt tokens), loop guard, and ClickHouse queries
│   ├── src/llm_client.rs            [REWRITTEN] Multi-turn tool loop, mandatory domain tool continuation guard, Google/Groq backoff parser
│   ├── src/state.rs                 [EXTENDED] Added ClickHouseWarehouse and feature state management to AppState
│   └── src/event_handler.rs         [REWIRED] Swapped static evaluate() call with propose_with_tools(); preserved idempotency filter
└── send_spike.py                    [EXTENDED] Dynamic SKU, location (e.g. BOM, BLR), and order ID argument parsing
```

---

## 2. Key Architecture Components & User Queries Addressed
## 2. Key Architecture Components & Design Decisions

### A. Model Selection: `llama-3.3-70b-versatile`
- **Default in Codebase**: Configured as default fallback for Groq in `config.rs`:
  ```rust
  env("GROQ_MODEL").unwrap_or_else(|| "llama-3.3-70b-versatile".to_owned())
  ```
- **Tool-Calling Support Level**: SOTA on Groq (Meta Llama 3.3 70B Instruct). Full 128k context window, native support for parallel function calling, and strict OpenAI-compatible `tools` schema.
- **Why Selected**:
  - `openai/gpt-oss-120b`: Reasoning tokens burst exceeded Groq's 8,000 TPM free tier, causing immediate 429 lockouts.
  - `qwen/qwen3.8-27b`: Fast, but lacks official Groq benchmark guarantees for multi-turn tool schema adherence.
  - `llama-3.3-70b-versatile`: Production-grade reliability, handles multi-tool outputs and JSON synthesis cleanly at ~280 tok/s.
### A. Multi-Provider Selection: Groq vs Gemini
- **Groq (`qwen/qwen3.8-27b`)**:
  - Ultra-low latency (~25ms–450ms).
  - Native parallel tool dispatch.
  - Compacted tool descriptions (808 prompt tokens) ensure multi-turn loops fit within Groq's 8,000 TPM free tier.
- **Gemini (`gemini-3.1-flash-lite`)**:
### A. Primary Model: `openai/gpt-oss-20b` (on Groq)
- **Selection Rationale:**
  - Fast execution (~40ms inference latency).
  - Built-in chain-of-thought (`reasoning_tokens`) providing clear step-by-step supply chain justification.
  - Native OpenAI-compatible tool calling on Groq with reliable parameter schema binding.
  - Fits comfortably within Groq Free Tier when paired with our compacted 808-token tool schemas.
- **Alternative High-Volume Option: `gemini-3.1-flash-lite` (on Google AI Studio)**:
  - High token capacity: **1,000,000 Tokens Per Minute (TPM)** and 15+ RPM on Google AI Studio Free Tier (125$\times$ higher than Groq).
  - Handles continuous back-to-back events without 429 throttling.

### B. ToolLoopGuard Configurable Threshold
- **Exact Threshold**: Configurable via `LLM_MAX_IDENTICAL_TOOL_CALLS` (default `1`).
- **Mechanism**: Tracks calls via `HashMap<(tool_name, normalized_arguments), count>`.
  - When threshold is `1`: Call #1 allowed; call #2 with identical arguments triggers guard and returns `status: "ERROR"`.
  - When threshold is set to `2`: Calls #1 and #2 allowed; call #3 triggers guard.
- **Arguments Normalization**: Normalizes JSON whitespace so `{"location_code":"NCR"}` and `{"location_code": "NCR"}` are recognized as identical.
### B. Mandatory Domain Tool Investigation (Continuation Guard)
- **The Issue:** Chhote models (like 20b or Qwen) often stopped calling tools after inspecting demand forecasts, skipping Indian festive calendars and supplier disruption signals.
- **The Issue:** Chhote models occasionally stopped calling tools after inspecting demand forecasts, skipping Indian festive calendars and supplier disruption signals.
- **The Solution:**
  1. System prompt specifies an explicit `MANDATORY INVESTIGATION SEQUENCE`.
  2. The Rust tool loop in `llm_client.rs` inspects `tools_used`. If either `get_event_calendar` or `get_supplier_signals` is missing when the model returns empty tool calls, the orchestrator injects a continuation prompt:
     ```text
     Before finalizing your replenishment decision, you are REQUIRED to query: [missing tools]. Please invoke these tools now.
     ```
  3. Drives the model to inspect festival and supplier data before synthesizing its final JSON decision.

### C. Dedicated Negative Test Cases (All 4 Verified)
The test suite now has dedicated automated tests explicitly covering the 4 negative-path scenarios requested:
1. **Malformed LLM JSON**:
   - `parse_action_json_rejects_malformed_json`: Rejects syntax errors, truncated objects, and missing required keys (`key_points`, `reorder_quantity`).
2. **Tool Call Timeout**:
   - `tool_execution_timeout_returns_error_status`: Verifies that a tool query exceeding `timeout` (5s) aborts and returns `ToolCallOutput` with `status: "ERROR"`.
3. **Loop-Guard Trigger on Repeated Identical Calls**:
   - `loop_guard_detects_repeated_identical_calls`: Tests threshold 1 (triggers on 2nd identical call) and threshold 2 (triggers on 3rd call).
4. **ClickHouse Unreachable $\to$ Tool Error $\to$ REVIEW Action**:
   - `unreachable_clickhouse_returns_error_status`: Verifies unreachable database returns `status: "ERROR"`.
   - `tool_error_results_in_review_action_selection`: Verifies that when tool returns `ERROR`, system prompt guides the LLM to choose `action: "REVIEW"` (with quantity 0 and confidence $\le 0.5$), which `SafetyFirewall` validates as a safe non-mutating review proposal.
### C. Deterministic Stockout Governance (`SafetyFirewall`)
- **The Issue:** Complete inventory depletion (`on_hand: 0`) was assessed as `LOW` risk by the LLM when forecasted demand was low, causing `PolicyGate` to auto-approve replenishment without human oversight.
- **The Guardrail:** If `on_hand <= 0` or `available <= 0` or `on_hand <= safety_stock`, and the LLM returned `LOW` risk, `SafetyFirewall` deterministically elevates the risk level to `MEDIUM`.
- **Policy Effect:** `PolicyGate` evaluates `RiskLevel::Medium => PolicyOutcome::RequiresHumanApproval`. Complete stockouts can **never** be silently auto-approved.

### D. Cost & Latency Impact Analysis
At current order volume, transitioning from a single-turn prompt to multi-turn tool calling has the following characteristics:
### D. Rate Limit Resilience & Retry-Delay Body Parser
Google AI Studio and Groq return retry durations in JSON error bodies or hints rather than standard `Retry-After` headers. `llm_client.rs` implements `parse_retry_delay`:
1. Parses standard `Retry-After` header if present.
2. Extracts `"retryDelay": "25s"` or `Please retry in 25.64s` from the error JSON body.
3. Adds safety buffers and sleeps the exact required duration before retrying.

| Metric | Single-Turn Baseline | Multi-Turn Tool Loop (2–3 Rounds) | Multiplier |
|---|---|---|---|
| **Input Tokens / Event** | ~600 tokens | ~3,400 tokens (accumulated context) | $\approx 5.6\times$ |
| **Output Tokens / Event** | ~150 tokens | ~360 tokens | $\approx 2.4\times$ |
| **Total Tokens / Event** | ~750 tokens | ~3,760 tokens | $\approx 5.0\times$ |
| **Est. Cost / Event (Llama 3.3 70B)** | ~\$0.00047 | ~\$0.0023 | $\approx 4.9\times$ |
| **Latency / Event** | ~0.6 seconds (1 HTTP call) | ~1.5s – 2.2s (2–3 LLM calls + Rust tools) | $\approx 2.5\times - 3.5\times$ |

**Mitigations Active in System**:
1. **Idempotency Deduplication**: In `event_handler.rs:265`, duplicated AMQP events are acknowledged and dropped immediately before any LLM execution occurs (0 tokens, \$0 cost).
2. **Steady-State No-Op Filter**: In `event_handler.rs:282`, events where stock is healthy and no spike is detected return `HandleOutcome::NoAction` without calling the LLM.
3. **Loop & Round Bounds**: Hard-capped by `LLM_MAX_TOOL_ROUNDS=6` and `LLM_MAX_IDENTICAL_TOOL_CALLS=1`.
4. **Circuit Breaker**: On 429 rate limit or network outage, fails over instantly to local Rust evaluation (0ms network delay, \$0 cost).

### E. System Prompt & Configuration Management
- `default_system_prompt()` defined in `config.rs` and loaded into `LlmConfig`.
- Configurable via `LLM_SYSTEM_PROMPT` (inline string) or `LLM_SYSTEM_PROMPT_FILE` (file path).
- Automatic `.env` file loader in `config.rs` reads local `.env` if present without requiring environment exports.
- `.env.example` created in `src/RustDataPlatform/.env.example` for secure credential storage.

---

## 3. Verification Results

### Automated Unit & Integration Tests
Ran `cargo test --workspace` across all crates in the workspace:
- Total tests: **58 passed; 0 failed**
- Formatting: `cargo fmt --all --check` clean (zero diff)
- Lints: `cargo clippy --workspace` clean (zero warnings, zero errors)
- Total tests: **66 passed; 0 failed** (31 in `hybrid-orchestrator`)
- Total tests: **67 passed; 0 failed** (33 in `hybrid-orchestrator`)
- Formatting (`cargo fmt --all --check`): Clean
- Clippy (`cargo clippy --workspace -- -D warnings`): Clean (0 warnings, 0 errors)

---

```
running 3 tests in cdc_consumer ... ok
running 5 tests in data_platform_common ... ok
running 7 tests in feature_engine ... ok
running 2 tests in governance ... ok
running 7 tests in lakehouse ... ok
running 10 tests in replenishment_agent ... ok
running 1 test in warehouse ... ok
running 23 tests in hybrid-orchestrator ... ok

test result: ok. 58 passed; 0 failed; 0 ignored; 0 measured; finished in 0.28s
```

### Code Quality & Static Analysis
- **`cargo clippy --workspace`**: Clean compilation with 0 warnings.
- **`cargo fmt --all --check`**: Clean formatting conforming to Rust style guidelines.

---

## 4. End-to-End Live Verification Guide

### Step 1: Start the Hybrid Orchestrator
In a dedicated terminal, launch the orchestrator with the updated environment configuration:
```bash
cargo run -p hybrid-orchestrator
```
Verify the startup log contains:
```text
LLM strategy officer initialized provider=Groq configured=true model="llama-3.3-70b-versatile"
running 31 tests in hybrid-orchestrator ...
running 33 tests in hybrid-orchestrator ...
test config::tests::from_env_loads_groq_when_set ... ok
test config::tests::groq_provider_is_configured_with_key ... ok
test config::tests::default_system_prompt_is_non_empty_and_mentions_json ... ok
test safety_firewall::tests::elevates_low_risk_to_medium_on_stockout ... ok
test safety_firewall::tests::elevates_low_risk_to_medium_when_at_or_below_safety_stock ... ok
test safety_firewall::tests::passes_valid_decision_within_bounds ... ok
test safety_firewall::tests::clamps_quantity_exceeding_capacity ... ok
test safety_firewall::tests::clamps_quantity_exceeding_budget_limit ... ok
test llm_tools::tests::statistical_analysis_computes_accurate_math ... ok
test llm_tools::tests::loop_guard_detects_repeated_identical_calls ... ok
test llm_client::tests::parse_retry_delay_prefers_http_retry_after_header ... ok
test llm_client::tests::circuit_breaker_falls_back_when_provider_is_unavailable ... ok
...
test result: ok. 66 passed; 0 failed; 0 ignored; 0 measured; finished in 0.01s
test result: ok. 67 passed; 0 failed; 0 ignored; 0 measured; finished in 0.01s
```

You can confirm health via curl:
```bash
curl -s http://127.0.0.1:5005/health | jq .
```
Expected response:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "executionMode": "LOG_ONLY",
  "llm": {
    "provider": "GROQ",
    "configured": true,
    "circuitOpen": false,
    "consecutiveFailures": 0,
    "model": "llama-3.3-70b-versatile"
  }
}
```

### Step 2: Publish a Spike / Deficit Event
In a second terminal:
```bash
source /home/cleaff/eShop/.venv-parquet/bin/activate
python3 /home/cleaff/eShop/send_spike.py critical 2 BLR
```

### Step 3: Inspect Multi-Turn Tool Trace and Proposal
Fetch the latest generated proposal:
```bash
curl -s http://127.0.0.1:5005/api/v1/proposals | jq '.[-1].proposal.reasoning'
```

### Step 4: Approve the Proposal
Use the `proposal_id` from the list:
```bash
PROPOSAL_ID=$(curl -s http://127.0.0.1:5005/api/v1/proposals | jq -r '.[-1].proposal.proposal_id')
curl -s -X POST http://127.0.0.1:5005/api/v1/proposals/${PROPOSAL_ID}/approve | jq .
```

