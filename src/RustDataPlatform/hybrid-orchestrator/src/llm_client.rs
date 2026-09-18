use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, bail};
use data_platform_common::InventoryBalanceState;
use replenishment_agent::{LlmReasoning, ReorderDecision, ReorderSimulation};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::warn;

use crate::config::{LlmConfig, LlmProvider};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmActionProposal {
    pub action: String,
    pub reorder_quantity: i32,
    pub markdown_percent: f64,
    pub urgency: String,
    pub summary: String,
    pub key_points: Vec<String>,
    pub confidence: f64,
}

impl LlmActionProposal {
    pub fn to_reasoning(&self) -> LlmReasoning {
        LlmReasoning {
            summary: self.summary.clone(),
            key_points: self.key_points.clone(),
            confidence: self.confidence,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HybridPolicyLimits {
    pub max_markdown_percent: f64,
    pub max_order_quantity: i32,
}

impl HybridPolicyLimits {
    pub fn from_config(config: &LlmConfig) -> Self {
        Self {
            max_markdown_percent: config.max_markdown_percent,
            max_order_quantity: config.max_order_quantity,
        }
    }

    pub fn validate_action(
        &self,
        action: &LlmActionProposal,
        balance: &InventoryBalanceState,
    ) -> Result<()> {
        if action.reorder_quantity < 0 {
            bail!("LLM proposed negative reorder quantity");
        }
        if action.reorder_quantity > self.max_order_quantity {
            bail!(
                "LLM proposed quantity {} above hard cap {}",
                action.reorder_quantity,
                self.max_order_quantity
            );
        }
        let capacity = (balance.max_stock - balance.on_hand).max(0);
        if action.reorder_quantity > capacity {
            bail!(
                "LLM proposed quantity {} above current capacity {}",
                action.reorder_quantity,
                capacity
            );
        }
        if action.markdown_percent < 0.0 || action.markdown_percent > self.max_markdown_percent {
            bail!(
                "LLM proposed markdown {:.2}% outside deterministic limit {:.2}%",
                action.markdown_percent,
                self.max_markdown_percent
            );
        }
        if action.summary.trim().is_empty() || action.key_points.is_empty() {
            bail!("LLM proposal missing summary/key_points");
        }
        if !(0.0..=1.0).contains(&action.confidence) {
            bail!("LLM confidence must be between 0 and 1");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmClientStatus {
    pub provider: LlmProvider,
    pub configured: bool,
    pub circuit_open: bool,
    pub consecutive_failures: u32,
    pub model: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmFallbackReason {
    Disabled,
    CircuitOpen,
    ProviderError,
    PolicyRejected,
}

#[derive(Debug, Clone)]
pub struct LlmDecision {
    pub reasoning: LlmReasoning,
    pub proposed_quantity: i32,
    pub fallback_reason: Option<LlmFallbackReason>,
}

#[derive(Debug)]
struct CircuitState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct LlmClient {
    http: reqwest::Client,
    config: Arc<LlmConfig>,
    circuit: Arc<Mutex<CircuitState>>,
}

impl LlmClient {
    pub fn new(config: LlmConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("reqwest client configuration should be valid");
        Self {
            http,
            config: Arc::new(config),
            circuit: Arc::new(Mutex::new(CircuitState {
                consecutive_failures: 0,
                opened_at: None,
            })),
        }
    }

    pub async fn status(&self) -> LlmClientStatus {
        let circuit = self.circuit.lock().await;
        LlmClientStatus {
            provider: self.config.provider,
            configured: self.config.configured(),
            circuit_open: self.is_open_locked(&circuit),
            consecutive_failures: circuit.consecutive_failures,
            model: self.config.model.clone(),
        }
    }

    pub async fn propose_or_fallback(
        &self,
        decision: &ReorderDecision,
        simulation: &ReorderSimulation,
        balance: &InventoryBalanceState,
    ) -> LlmDecision {
        let deterministic = deterministic_reasoning(decision, simulation);
        if !self.config.configured() {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: simulation.selected_quantity,
                fallback_reason: Some(LlmFallbackReason::Disabled),
            };
        }
        if self.circuit_is_open().await {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: simulation.selected_quantity,
                fallback_reason: Some(LlmFallbackReason::CircuitOpen),
            };
        }

        match self.call_provider(decision, simulation, balance).await {
            Ok(action) => {
                let limits = HybridPolicyLimits::from_config(&self.config);
                if let Err(error) = limits.validate_action(&action, balance) {
                    warn!(
                        ?error,
                        "LLM proposal rejected by deterministic hybrid policy boundary"
                    );
                    self.record_failure().await;
                    return LlmDecision {
                        reasoning: deterministic,
                        proposed_quantity: simulation.selected_quantity,
                        fallback_reason: Some(LlmFallbackReason::PolicyRejected),
                    };
                }
                self.record_success().await;
                LlmDecision {
                    reasoning: action.to_reasoning(),
                    proposed_quantity: action.reorder_quantity,
                    fallback_reason: None,
                }
            }
            Err(error) => {
                warn!(
                    ?error,
                    "LLM provider failed; degrading to deterministic proposal"
                );
                self.record_failure().await;
                LlmDecision {
                    reasoning: deterministic,
                    proposed_quantity: simulation.selected_quantity,
                    fallback_reason: Some(LlmFallbackReason::ProviderError),
                }
            }
        }
    }

    async fn call_provider(
        &self,
        decision: &ReorderDecision,
        simulation: &ReorderSimulation,
        balance: &InventoryBalanceState,
    ) -> Result<LlmActionProposal> {
        let key = self
            .config
            .api_key
            .as_deref()
            .context("LLM API key missing")?;
        let request = json!({
            "model": self.config.model,
            "temperature": 0.1,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a retail supply-chain strategy officer. Return only JSON that matches the propose_action schema. Do not request tools, databases, APIs, shell, or filesystem access. Deterministic governance will validate your proposal."
                },
                {
                    "role": "user",
                    "content": serde_json::to_string(&json!({
                        "task": "propose_action",
                        "decision": decision,
                        "simulation": simulation,
                        "balance": balance,
                        "hard_limits": {
                            "max_markdown_percent": self.config.max_markdown_percent,
                            "max_order_quantity": self.config.max_order_quantity,
                            "current_capacity": (balance.max_stock - balance.on_hand).max(0)
                        }
                    }))?
                }
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "propose_action",
                    "strict": true,
                    "schema": propose_action_schema()
                }
            }
        });

        let response = self
            .http
            .post(&self.config.endpoint)
            .bearer_auth(key)
            .json(&request)
            .send()
            .await
            .context("LLM HTTP request failed")?;

        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            bail!("LLM provider rate limited request with HTTP 429");
        }
        if !response.status().is_success() {
            bail!("LLM provider returned HTTP {}", response.status());
        }

        let envelope: ChatCompletionResponse =
            response.json().await.context("LLM response JSON failed")?;
        let content = envelope
            .choices
            .first()
            .context("LLM response contained no choices")?
            .message
            .content
            .as_deref()
            .context("LLM response content was empty")?;
        parse_action_json(content)
    }

    async fn circuit_is_open(&self) -> bool {
        let mut circuit = self.circuit.lock().await;
        if let Some(opened_at) = circuit.opened_at {
            if opened_at.elapsed() >= self.config.circuit_cooldown {
                circuit.opened_at = None;
                circuit.consecutive_failures = 0;
                return false;
            }
            return true;
        }
        false
    }

    fn is_open_locked(&self, circuit: &CircuitState) -> bool {
        circuit
            .opened_at
            .is_some_and(|opened_at| opened_at.elapsed() < self.config.circuit_cooldown)
    }

    async fn record_failure(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures += 1;
        if circuit.consecutive_failures >= self.config.circuit_failure_threshold {
            circuit.opened_at = Some(Instant::now());
        }
    }

    async fn record_success(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures = 0;
        circuit.opened_at = None;
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Option<String>,
}

pub fn parse_action_json(value: &str) -> Result<LlmActionProposal> {
    let proposal: LlmActionProposal =
        serde_json::from_str(value).context("LLM action JSON parse failed")?;
    if proposal.summary.trim().is_empty()
        || proposal.key_points.is_empty()
        || !(0.0..=1.0).contains(&proposal.confidence)
    {
        bail!("LLM action failed required field validation");
    }
    Ok(proposal)
}

fn deterministic_reasoning(
    decision: &ReorderDecision,
    simulation: &ReorderSimulation,
) -> LlmReasoning {
    LlmReasoning {
        summary: format!(
            "Deterministic fallback recommends {} units for SKU {} at {} because policy found {:?} risk and simulation selected the best capacity-safe alternative.",
            simulation.selected_quantity,
            decision.sku_id,
            decision.location_code,
            decision.risk_level
        ),
        key_points: decision.reason_codes.clone(),
        confidence: 0.80,
    }
}

fn propose_action_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["action", "reorder_quantity", "markdown_percent", "urgency", "summary", "key_points", "confidence"],
        "properties": {
            "action": { "type": "string", "enum": ["REORDER", "WAIT", "REVIEW"] },
            "reorder_quantity": { "type": "integer", "minimum": 0 },
            "markdown_percent": { "type": "number", "minimum": 0, "maximum": 100 },
            "urgency": { "type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"] },
            "summary": { "type": "string", "minLength": 1 },
            "key_points": { "type": "array", "minItems": 1, "items": { "type": "string" } },
            "confidence": { "type": "number", "minimum": 0, "maximum": 1 }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use data_platform_common::{InventoryBalanceState, SkuLocation};
    use feature_engine::{AdaptiveForecast, SkuLocationFeatures};
    use replenishment_agent::{
        AgentMode, ReplenishmentAgent, ReplenishmentPolicyConfig, SimulationEngine,
    };

    use super::*;
    use crate::config::{LlmConfig, LlmProvider};

    #[test]
    fn llm_json_schema_parser_accepts_valid_action() {
        let json = r#"{
            "action":"REORDER",
            "reorder_quantity":12,
            "markdown_percent":0,
            "urgency":"HIGH",
            "summary":"Reorder now due to low coverage.",
            "key_points":["LOW_COVERAGE"],
            "confidence":0.91
        }"#;
        let parsed = parse_action_json(json).unwrap();
        assert_eq!(parsed.reorder_quantity, 12);
        assert_eq!(parsed.action, "REORDER");
    }

    #[tokio::test]
    async fn circuit_breaker_fallbacks_when_provider_is_unavailable() {
        let config = LlmConfig {
            provider: LlmProvider::Groq,
            api_key: Some("test-key".to_owned()),
            model: "test-model".to_owned(),
            endpoint: "http://127.0.0.1:9/v1/chat/completions".to_owned(),
            timeout: Duration::from_millis(100),
            circuit_failure_threshold: 1,
            circuit_cooldown: Duration::from_secs(60),
            max_markdown_percent: 20.0,
            max_order_quantity: 100,
        };
        let client = LlmClient::new(config);
        let (decision, simulation, balance) = sample_decision();

        let result = client
            .propose_or_fallback(&decision, &simulation, &balance)
            .await;

        assert_eq!(
            result.fallback_reason,
            Some(LlmFallbackReason::ProviderError)
        );
        assert_eq!(result.proposed_quantity, simulation.selected_quantity);
        assert!(client.status().await.circuit_open);
    }

    #[test]
    fn policy_boundary_rejects_exaggerated_llm_order_quantity() {
        let (_, _, balance) = sample_decision();
        let limits = HybridPolicyLimits {
            max_markdown_percent: 20.0,
            max_order_quantity: 50,
        };
        let action = LlmActionProposal {
            action: "REORDER".to_owned(),
            reorder_quantity: 10_000,
            markdown_percent: 0.0,
            urgency: "CRITICAL".to_owned(),
            summary: "Buy everything".to_owned(),
            key_points: vec!["ROGUE_QUANTITY".to_owned()],
            confidence: 0.99,
        };

        assert!(limits.validate_action(&action, &balance).is_err());
    }

    fn sample_decision() -> (
        replenishment_agent::ReorderDecision,
        ReorderSimulation,
        InventoryBalanceState,
    ) {
        let now = Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0).unwrap();
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
            updated_at: now,
        };
        let features = SkuLocationFeatures {
            key: SkuLocation::new(42, "NCR"),
            computed_at: now,
            on_hand: 50,
            reserved: 0,
            available: 50,
            balance_version: 1,
            reserve_units_5m: 0,
            sale_units_5m: 10,
            sale_units_15m: 20,
            sale_units_1h: 30,
            sale_velocity_5m: 50.0,
            sale_velocity_15m: 40.0,
            sale_velocity_1h: 30.0,
            daily_sales: 30,
            forecast: AdaptiveForecast {
                baseline_units_per_day: 10.0,
                recent_units_per_day: 50.0,
                forecast_units_per_day: 50.0,
                historical_stddev_units_per_day: 3.0,
                recent_vs_baseline_ratio: 5.0,
                spike_score: 13.0,
                spike_detected: true,
            },
            forecast_horizon_days: Vec::new(),
        };
        let agent =
            ReplenishmentAgent::new(ReplenishmentPolicyConfig::default(), AgentMode::Recommend);
        let decision = agent.evaluate(&features, &balance, 0);
        let simulation =
            SimulationEngine::compare_reorder_quantities(&decision, &balance, 0, &[0, 25, 50]);
        (decision, simulation, balance)
    }
}
