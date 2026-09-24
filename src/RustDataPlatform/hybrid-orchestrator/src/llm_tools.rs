use std::{collections::HashMap, time::Duration};

use chrono::Utc;
use data_platform_common::InventoryBalanceState;
use feature_engine::{SkuLocationFeatures, SkuLocationState};
use replenishment_agent::{ReorderDecision, SimulationEngine};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::warn;
use warehouse::{AnalyticalWarehouse, ClickHouseWarehouse};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallOutput {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ToolCallOutput {
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            status: "SUCCESS".to_owned(),
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            status: "ERROR".to_owned(),
            data: None,
            error: Some(msg.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatisticalAnalysisResult {
    pub count: usize,
    pub mean: f64,
    pub stddev: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub trend_slope: f64,
}

pub fn calculate_statistics(values: &[f64]) -> Result<StatisticalAnalysisResult, String> {
    if values.is_empty() {
        return Err("cannot compute statistics on empty array".to_owned());
    }

    let n = values.len();
    let sum: f64 = values.iter().sum();
    let mean = sum / n as f64;

    let variance = if n > 1 {
        let sum_sq_diff: f64 = values.iter().map(|&x| (x - mean).powi(2)).sum();
        sum_sq_diff / (n - 1) as f64
    } else {
        0.0
    };
    let stddev = variance.sqrt();

    let min = values
        .iter()
        .copied()
        .fold(f64::INFINITY, |a, b| if a < b { a } else { b });
    let max = values
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, |a, b| if a > b { a } else { b });

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    };

    // Linear regression trend slope: x_i = 0..n-1, y_i = values[i]
    let trend_slope = if n > 1 {
        let t_mean = (n - 1) as f64 / 2.0;
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        for (i, &y) in values.iter().enumerate() {
            let t_diff = i as f64 - t_mean;
            numerator += t_diff * (y - mean);
            denominator += t_diff * t_diff;
        }
        if denominator.abs() > 1e-9 {
            numerator / denominator
        } else {
            0.0
        }
    } else {
        0.0
    };

    Ok(StatisticalAnalysisResult {
        count: n,
        mean,
        stddev,
        min,
        max,
        median,
        trend_slope,
    })
}

#[derive(Debug, Clone)]
pub struct ToolLoopGuard {
    seen_calls: HashMap<(String, String), usize>,
    max_identical_calls: usize,
}

impl Default for ToolLoopGuard {
    fn default() -> Self {
        Self::new(1)
    }
}

impl ToolLoopGuard {
    pub fn new(max_identical_calls: usize) -> Self {
        Self {
            seen_calls: HashMap::new(),
            max_identical_calls: max_identical_calls.max(1),
        }
    }

    pub fn check_and_record(&mut self, tool_name: &str, arguments: &str) -> Result<(), String> {
        let normalized_args = match serde_json::from_str::<serde_json::Value>(arguments) {
            Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| arguments.trim().to_owned()),
            Err(_) => arguments.trim().to_owned(),
        };
        let key = (tool_name.to_owned(), normalized_args);
        let count = self.seen_calls.entry(key).or_insert(0);
        *count += 1;
        if *count > self.max_identical_calls {
            return Err(format!(
                "Repeated identical tool call detected (count {} > allowed {}): '{}' with arguments '{}'. \
                 You already called this tool with these exact arguments in this decision cycle. \
                 Do not repeat identical calls. Synthesize from existing observations or choose action REVIEW.",
                *count, self.max_identical_calls, tool_name, arguments
            ));
        }
        Ok(())
    }
}

pub struct ToolExecutionContext<'a> {
    pub warehouse: &'a ClickHouseWarehouse,
    pub current_balance: &'a InventoryBalanceState,
    pub current_features: Option<&'a SkuLocationFeatures>,
    pub feature_state: Option<&'a Mutex<SkuLocationState>>,
    pub base_decision: &'a ReorderDecision,
    pub timeout: Duration,
    pub macro_cache: Option<&'a crate::macro_cache::MacroSignalCache>,
}

pub async fn execute_tool(
    name: &str,
    args_json: &str,
    ctx: &ToolExecutionContext<'_>,
) -> ToolCallOutput {
    let execution = async {
        let args: serde_json::Value = serde_json::from_str(args_json)
            .map_err(|e| format!("Failed to parse arguments JSON: {e}"))?;

        match name {
            "get_inventory_snapshot" => {
                let sku_id =
                    args.get("sku_id")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(ctx.current_balance.sku_id as i64) as i32;
                let location_code = args
                    .get("location_code")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&ctx.current_balance.location_code)
                    .trim()
                    .to_ascii_uppercase();

                if sku_id == ctx.current_balance.sku_id
                    && location_code == ctx.current_balance.location_code
                {
                    Ok(json!(ctx.current_balance))
                } else {
                    let recovery = ctx
                        .warehouse
                        .load_recovery_state()
                        .await
                        .map_err(|e| format!("Warehouse query failed: {e}"))?;
                    let found = recovery
                        .balances
                        .into_iter()
                        .find(|b| b.sku_id == sku_id && b.location_code == location_code);
                    Ok(json!(found))
                }
            }
            "get_demand_features" => {
                if let Some(features) = ctx.current_features {
                    Ok(json!({
                        "sku_id": features.key.sku_id,
                        "location_code": features.key.location_code,
                        "computed_at": features.computed_at,
                        "on_hand": features.on_hand,
                        "reserved": features.reserved,
                        "available": features.available,
                        "sale_units_5m": features.sale_units_5m,
                        "sale_units_15m": features.sale_units_15m,
                        "sale_units_1h": features.sale_units_1h,
                        "sale_velocity_5m_units_per_day": features.sale_velocity_5m,
                        "sale_velocity_15m_units_per_day": features.sale_velocity_15m,
                        "sale_velocity_1h_units_per_day": features.sale_velocity_1h,
                        "daily_sales": features.daily_sales,
                        "baseline_units_per_day": features.forecast.baseline_units_per_day,
                        "recent_units_per_day": features.forecast.recent_units_per_day,
                        "forecast_units_per_day": features.forecast.forecast_units_per_day,
                        "historical_stddev_units_per_day": features.forecast.historical_stddev_units_per_day,
                        "recent_vs_baseline_ratio": features.forecast.recent_vs_baseline_ratio,
                        "spike_score": features.forecast.spike_score,
                        "spike_detected": features.forecast.spike_detected,
                    }))
                } else {
                    Err("No real-time demand features available for this SKU/location".to_owned())
                }
            }
            "get_demand_forecast" => {
                let horizon_days = args
                    .get("horizon_days")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(7) as usize;
                if let Some(state_mutex) = ctx.feature_state {
                    let state = state_mutex.lock().await;
                    let now = Utc::now();
                    let v5 = ctx
                        .current_features
                        .map(|f| f.sale_velocity_5m)
                        .unwrap_or(0.0);
                    let v15 = ctx
                        .current_features
                        .map(|f| f.sale_velocity_15m)
                        .unwrap_or(0.0);
                    let v1h = ctx
                        .current_features
                        .map(|f| f.sale_velocity_1h)
                        .unwrap_or(0.0);
                    let points = state
                        .model
                        .forecast_horizon(now, horizon_days, v5, v15, v1h);
                    Ok(json!(points))
                } else if let Some(features) = ctx.current_features {
                    Ok(json!(features.forecast_horizon_days))
                } else {
                    Err("No forecast model available for this SKU/location".to_owned())
                }
            }
            "get_historical_sales" => {
                let sku_id =
                    args.get("sku_id")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(ctx.current_balance.sku_id as i64) as i32;
                let location_code = args
                    .get("location_code")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&ctx.current_balance.location_code);
                let days_back = args.get("days_back").and_then(|v| v.as_u64()).unwrap_or(30) as u32;

                let entries = ctx
                    .warehouse
                    .query_historical_daily_sales(sku_id, location_code, days_back)
                    .await
                    .map_err(|e| format!("ClickHouse query failed: {e}"))?;
                Ok(json!(entries))
            }
            "run_statistical_analysis" => {
                let raw_values = args
                    .get("values")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| "Missing required 'values' array parameter".to_owned())?;
                let mut values = Vec::with_capacity(raw_values.len());
                for item in raw_values {
                    if let Some(num) = item.as_f64() {
                        values.push(num);
                    } else if let Some(int_num) = item.as_i64() {
                        values.push(int_num as f64);
                    } else {
                        return Err("Array contains non-numeric element".to_owned());
                    }
                }
                let stats = calculate_statistics(&values)?;
                Ok(json!(stats))
            }
            "simulate_reorder" => {
                let raw_candidates = args
                    .get("candidate_quantities")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        "Missing required 'candidate_quantities' array parameter".to_owned()
                    })?;
                let mut candidates = Vec::with_capacity(raw_candidates.len());
                for item in raw_candidates {
                    let qty = item
                        .as_i64()
                        .ok_or_else(|| "Candidate quantity must be integer".to_owned())?
                        as i32;
                    candidates.push(qty);
                }
                if candidates.is_empty() {
                    return Err("Candidate quantities array must not be empty".to_owned());
                }
                candidates.sort_unstable();
                candidates.dedup();

                let simulation = SimulationEngine::compare_reorder_quantities(
                    ctx.base_decision,
                    ctx.current_balance,
                    0,
                    &candidates,
                );
                Ok(json!(simulation))
            }
            "get_event_calendar" => {
                let location_code = args
                    .get("location_code")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&ctx.current_balance.location_code);
                let days_ahead = args
                    .get("days_ahead")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(60) as u32;

                let events = if let Some(cache) = ctx.macro_cache {
                    cache
                        .get_event_calendar(location_code, days_ahead, ctx.warehouse)
                        .await
                        .map_err(|e| format!("ClickHouse event_calendar query failed: {e}"))?
                } else {
                    ctx.warehouse
                        .query_event_calendar(location_code, days_ahead)
                        .await
                        .map_err(|e| format!("ClickHouse event_calendar query failed: {e}"))?
                };
                Ok(json!(events))
            }
            "get_supplier_signals" => {
                let location_code = args
                    .get("location_code")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&ctx.current_balance.location_code);
                let sku_id = args
                    .get("sku_id")
                    .and_then(|v| v.as_i64())
                    .map(|id| id as i32);

                let signals = if let Some(cache) = ctx.macro_cache {
                    cache
                        .get_supplier_signals(sku_id, location_code, ctx.warehouse)
                        .await
                        .map_err(|e| format!("ClickHouse supplier_signals query failed: {e}"))?
                } else {
                    ctx.warehouse
                        .query_supplier_signals(sku_id, location_code)
                        .await
                        .map_err(|e| format!("ClickHouse supplier_signals query failed: {e}"))?
                };
                Ok(json!(signals))
            }
            other => Err(format!("Unknown tool '{other}'")),
        }
    };

    match tokio::time::timeout(ctx.timeout, execution).await {
        Ok(Ok(value)) => ToolCallOutput::success(value),
        Ok(Err(err_msg)) => {
            warn!(tool = name, error = %err_msg, "Tool execution returned error");
            ToolCallOutput::error(err_msg)
        }
        Err(_) => {
            let msg = format!("Tool execution timed out after {:?}", ctx.timeout);
            warn!(tool = name, timeout = ?ctx.timeout, "Tool execution timed out");
            ToolCallOutput::error(msg)
        }
    }
}

pub fn tool_definitions() -> Vec<serde_json::Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "get_inventory_snapshot",
                "description": "Get current live inventory balances (on_hand, reserved, available, safety_stock, reorder_point, max_stock) for a SKU at a warehouse location.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sku_id": { "type": "integer", "description": "The SKU product ID (e.g. 2, 42)" },
                        "location_code": { "type": "string", "description": "Warehouse location code (e.g. NCR, BLR, BOM, HYD)" }
                    },
                    "required": ["sku_id", "location_code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "get_demand_features",
                "description": "Get real-time demand signals: rolling sale velocities (5-minute, 15-minute, 1-hour annualized rates), daily sales, EWMA baseline demand, and automated spike detection flags.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sku_id": { "type": "integer", "description": "The SKU product ID" },
                        "location_code": { "type": "string", "description": "Warehouse location code" }
                    },
                    "required": ["sku_id", "location_code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "get_demand_forecast",
                "description": "Get day-by-day forecasted demand units over the upcoming lead-time and review period horizon, taking into account weekday seasonality patterns.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sku_id": { "type": "integer", "description": "The SKU product ID" },
                        "location_code": { "type": "string", "description": "Warehouse location code" },
                        "horizon_days": { "type": "integer", "description": "Number of days to project forward (default 7)" }
                    },
                    "required": ["sku_id", "location_code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "get_historical_sales",
                "description": "Query historical daily sales totals from ClickHouse fact tables to inspect trends and seasonality.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sku_id": { "type": "integer", "description": "The SKU product ID" },
                        "location_code": { "type": "string", "description": "Warehouse location code" },
                        "days_back": { "type": "integer", "description": "Number of past days to query (default 30)" }
                    },
                    "required": ["sku_id", "location_code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "run_statistical_analysis",
                "description": "Run deterministic arithmetic on a numeric series: computes count, mean, sample standard deviation, min, max, median, and linear trend slope. Use this tool instead of doing manual arithmetic.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "values": {
                            "type": "array",
                            "items": { "type": "number" },
                            "description": "Array of numeric values to analyze"
                        }
                    },
                    "required": ["values"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "simulate_reorder",
                "description": "Run inventory simulation across candidate reorder quantities to evaluate stockout risks, excess inventory, and demand coverage ratios.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "candidate_quantities": {
                            "type": "array",
                            "items": { "type": "integer" },
                            "description": "List of candidate reorder batch sizes to simulate (e.g. [0, 25, 50, 100])"
                        }
                    },
                    "required": ["candidate_quantities"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "get_event_calendar",
                "description": "Check the calendar for upcoming promotions, festive spikes (Diwali, Great Indian Festival, etc.), holidays, or seasonal shifts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location_code": { "type": "string", "description": "Warehouse location code (e.g. NCR, BLR, BOM)" },
                        "days_ahead": { "type": "integer", "description": "Days forward to check (default 60)" }
                    },
                    "required": ["location_code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "get_supplier_signals",
                "description": "Query active supply-chain disruption signals (supplier delays, raw material shortages, quality blocks, transport bottlenecks).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location_code": { "type": "string", "description": "Warehouse location code" },
                        "sku_id": { "type": ["integer", "null"], "description": "Optional SKU ID filter; pass null for warehouse-wide signals" }
                    },
                    "required": ["location_code"]
                }
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistical_analysis_computes_accurate_math() {
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let stats = calculate_statistics(&values).unwrap();
        assert_eq!(stats.count, 5);
        assert_eq!(stats.mean, 30.0);
        assert!((stats.stddev - 15.811388).abs() < 1e-4);
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 50.0);
        assert_eq!(stats.median, 30.0);
        assert!((stats.trend_slope - 10.0).abs() < 1e-6);
    }

    #[test]
    fn statistical_analysis_rejects_empty_array() {
        assert!(calculate_statistics(&[]).is_err());
    }

    #[test]
    fn loop_guard_detects_repeated_identical_calls() {
        let mut guard = ToolLoopGuard::new(1);
        assert!(
            guard
                .check_and_record("get_event_calendar", r#"{"location_code":"NCR"}"#)
                .is_ok()
        );
        assert!(
            guard
                .check_and_record("get_event_calendar", r#"{"location_code":"BLR"}"#)
                .is_ok()
        );
        // Same arguments in different whitespace formatting triggers guard on 2nd call
        assert!(
            guard
                .check_and_record("get_event_calendar", r#"{ "location_code": "NCR" }"#)
                .is_err()
        );

        // Configurable threshold: max_identical_calls = 2 allows 2, fails on 3rd
        let mut guard2 = ToolLoopGuard::new(2);
        assert!(
            guard2
                .check_and_record("get_event_calendar", r#"{"location_code":"NCR"}"#)
                .is_ok()
        );
        assert!(
            guard2
                .check_and_record("get_event_calendar", r#"{"location_code":"NCR"}"#)
                .is_ok()
        );
        assert!(
            guard2
                .check_and_record("get_event_calendar", r#"{"location_code":"NCR"}"#)
                .is_err()
        );
    }

    #[test]
    fn tool_definitions_are_valid_json() {
        let defs = tool_definitions();
        assert_eq!(defs.len(), 8);
        for def in defs {
            assert_eq!(def["type"], "function");
            assert!(def["function"]["name"].is_string());
            assert!(def["function"]["parameters"]["properties"].is_object());
        }
    }

    #[tokio::test]
    async fn unreachable_clickhouse_returns_error_status() {
        let bad_ch = ClickHouseWarehouse::new(warehouse::ClickHouseConfig {
            url: "http://127.0.0.1:1".to_owned(),
            database: "test".to_owned(),
            user: "test".to_owned(),
            password: "test".to_owned(),
        });
        let balance = InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: 50,
            reserved: 0,
            available: 50,
            safety_stock: 10,
            reorder_point: 20,
            max_stock: 100,
            version: 1,
            updated_at: Utc::now(),
        };
        let decision = replenishment_agent::ReorderDecision {
            decision_id: uuid::Uuid::new_v4(),
            sku_id: 42,
            location_code: "NCR".to_owned(),
            created_at: Utc::now(),
            forecast_units_per_day: 10.0,
            baseline_units_per_day: 10.0,
            recent_units_per_day: 10.0,
            spike_score: 0.0,
            risk_level: replenishment_agent::RiskLevel::Low,
            estimated_stockout_at: None,
            inventory_position: 50,
            safety_stock: 10,
            target_inventory: 60,
            recommended_quantity: 10,
            horizon_demand_units: 30.0,
            horizon_forecast: Vec::new(),
            reason_codes: Vec::new(),
            model_version: "test".to_owned(),
            mode: replenishment_agent::AgentMode::Recommend,
        };
        let ctx = ToolExecutionContext {
            warehouse: &bad_ch,
            current_balance: &balance,
            current_features: None,
            feature_state: None,
            base_decision: &decision,
            timeout: Duration::from_millis(500),
            macro_cache: None,
        };

        let result = execute_tool(
            "get_historical_sales",
            r#"{"sku_id": 42, "location_code": "NCR", "days_back": 7}"#,
            &ctx,
        )
        .await;
        assert_eq!(result.status, "ERROR");
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn tool_execution_timeout_returns_error_status() {
        let bad_ch = ClickHouseWarehouse::new(warehouse::ClickHouseConfig {
            url: "http://10.255.255.1:8123".to_owned(),
            database: "test".to_owned(),
            user: "test".to_owned(),
            password: "test".to_owned(),
        });
        let balance = InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: 50,
            reserved: 0,
            available: 50,
            safety_stock: 10,
            reorder_point: 20,
            max_stock: 100,
            version: 1,
            updated_at: Utc::now(),
        };
        let decision = replenishment_agent::ReorderDecision {
            decision_id: uuid::Uuid::new_v4(),
            sku_id: 42,
            location_code: "NCR".to_owned(),
            created_at: Utc::now(),
            forecast_units_per_day: 10.0,
            baseline_units_per_day: 10.0,
            recent_units_per_day: 10.0,
            spike_score: 0.0,
            risk_level: replenishment_agent::RiskLevel::Low,
            estimated_stockout_at: None,
            inventory_position: 50,
            safety_stock: 10,
            target_inventory: 60,
            recommended_quantity: 10,
            horizon_demand_units: 30.0,
            horizon_forecast: Vec::new(),
            reason_codes: Vec::new(),
            model_version: "test".to_owned(),
            mode: replenishment_agent::AgentMode::Recommend,
        };
        let ctx = ToolExecutionContext {
            warehouse: &bad_ch,
            current_balance: &balance,
            current_features: None,
            feature_state: None,
            base_decision: &decision,
            timeout: Duration::from_millis(50),
            macro_cache: None,
        };

        let result = execute_tool(
            "get_event_calendar",
            r#"{"location_code": "NCR", "days_ahead": 30}"#,
            &ctx,
        )
        .await;
        assert_eq!(result.status, "ERROR");
        assert!(result.error.is_some());
    }
}
