use chrono::{DateTime, Duration, Utc};
use data_platform_common::{InventoryBalanceState, SkuLocation};
use feature_engine::{DailyForecastPoint, SkuLocationFeatures};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentMode {
    #[default]
    Observe,
    Recommend,
    AutoDemo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct ReplenishmentPolicyConfig {
    pub lead_time_days: f64,
    pub review_period_days: f64,
    pub service_factor: f64,
    pub medium_risk_hours: f64,
    pub high_risk_hours: f64,
    pub critical_risk_hours: f64,
    pub model_version: String,
}

impl Default for ReplenishmentPolicyConfig {
    fn default() -> Self {
        Self {
            lead_time_days: 2.0,
            review_period_days: 1.0,
            service_factor: 1.65,
            medium_risk_hours: 96.0,
            high_risk_hours: 48.0,
            critical_risk_hours: 24.0,
            model_version: "adaptive-ewma-seasonal-v1".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReorderDecision {
    pub decision_id: Uuid,
    pub sku_id: i32,
    pub location_code: String,
    pub created_at: DateTime<Utc>,
    pub forecast_units_per_day: f64,
    pub baseline_units_per_day: f64,
    pub recent_units_per_day: f64,
    pub spike_score: f64,
    pub risk_level: RiskLevel,
    pub estimated_stockout_at: Option<DateTime<Utc>>,
    pub inventory_position: i32,
    pub safety_stock: i32,
    pub target_inventory: i32,
    pub recommended_quantity: i32,
    pub horizon_demand_units: f64,
    pub horizon_forecast: Vec<DailyForecastPoint>,
    pub reason_codes: Vec<String>,
    pub model_version: String,
    pub mode: AgentMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataFreshnessStatus {
    Final,
    Provisional,
    Stale,
    Unreconciled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResponse<T> {
    pub value: T,
    pub source: String,
    pub as_of: DateTime<Utc>,
    pub freshness: String,
    pub confidence: f64,
    pub status: DataFreshnessStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimulationAlternative {
    pub quantity: i32,
    pub projected_inventory_position: i32,
    pub projected_available_after_restock: i32,
    pub expected_horizon_demand: f64,
    pub expected_stockout_units: f64,
    pub expected_excess_units: f64,
    pub demand_coverage_ratio: f64,
    pub capacity_violation_units: i32,
    pub service_level_score: f64,
    pub risk_level: RiskLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReorderSimulation {
    pub simulation_id: Uuid,
    pub decision_id: Uuid,
    pub sku_id: i32,
    pub location_code: String,
    pub evaluated_at: DateTime<Utc>,
    pub selected_quantity: i32,
    pub alternatives: Vec<SimulationAlternative>,
}

pub struct SimulationEngine;

impl SimulationEngine {
    pub fn compare_reorder_quantities(
        decision: &ReorderDecision,
        balance: &InventoryBalanceState,
        incoming_replenishment: i32,
        candidate_quantities: &[i32],
    ) -> ReorderSimulation {
        let inventory_position = balance.on_hand - balance.reserved + incoming_replenishment;
        let capacity = (balance.max_stock - balance.on_hand - incoming_replenishment).max(0);
        let expected_horizon_demand = decision.horizon_demand_units.max(0.0);
        let alternatives = candidate_quantities
            .iter()
            .copied()
            .map(|quantity| {
                let accepted_quantity = quantity.clamp(0, capacity);
                let projected_inventory_position = inventory_position + accepted_quantity;
                let projected_available_after_restock = balance.available + accepted_quantity;
                let expected_stockout_units =
                    (expected_horizon_demand - projected_available_after_restock as f64).max(0.0);
                let expected_excess_units = (projected_inventory_position as f64
                    - decision.target_inventory as f64)
                    .max(0.0);
                let demand_coverage_ratio = if expected_horizon_demand <= 0.0 {
                    1.0
                } else {
                    (projected_available_after_restock as f64 / expected_horizon_demand)
                        .clamp(0.0, 1.5)
                };
                let service_level_score = (1.0
                    - expected_stockout_units / expected_horizon_demand.max(1.0))
                .clamp(0.0, 1.0);
                let risk_level = if expected_stockout_units <= 0.0 {
                    RiskLevel::Low
                } else if demand_coverage_ratio >= 0.75 {
                    RiskLevel::Medium
                } else if demand_coverage_ratio >= 0.5 {
                    RiskLevel::High
                } else {
                    RiskLevel::Critical
                };

                SimulationAlternative {
                    quantity,
                    projected_inventory_position,
                    projected_available_after_restock,
                    expected_horizon_demand,
                    expected_stockout_units,
                    expected_excess_units,
                    demand_coverage_ratio,
                    capacity_violation_units: quantity.saturating_sub(capacity),
                    service_level_score,
                    risk_level,
                }
            })
            .collect::<Vec<_>>();

        let selected_quantity = alternatives
            .iter()
            .min_by(|left, right| {
                left.expected_stockout_units
                    .partial_cmp(&right.expected_stockout_units)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        left.expected_excess_units
                            .partial_cmp(&right.expected_excess_units)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            })
            .map(|alternative| alternative.quantity.clamp(0, capacity))
            .unwrap_or(0);
        let identity = format!(
            "{}:{}:{}:{:?}",
            decision.decision_id, balance.version, incoming_replenishment, candidate_quantities
        );

        ReorderSimulation {
            simulation_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
            decision_id: decision.decision_id,
            sku_id: decision.sku_id,
            location_code: decision.location_code.clone(),
            evaluated_at: decision.created_at,
            selected_quantity,
            alternatives,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallRecord {
    pub tool_name: String,
    pub input: String,
    pub output: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmReasoning {
    pub summary: String,
    pub key_points: Vec<String>,
    pub confidence: f64,
    #[serde(default)]
    pub tools_used: Vec<String>,
    #[serde(default)]
    pub tool_calls: Vec<ToolCallRecord>,
}

#[derive(Debug, Error, PartialEq)]
pub enum LlmReasoningError {
    #[error("LLM output was malformed: {0}")]
    Malformed(String),
    #[error("LLM provider unavailable")]
    Unavailable,
}

pub trait LlmReasoner: Send + Sync {
    fn explain(
        &self,
        decision: &ReorderDecision,
        simulation: &ReorderSimulation,
    ) -> Result<LlmReasoning, LlmReasoningError>;
}

pub struct MockLlmReasoner;

impl LlmReasoner for MockLlmReasoner {
    fn explain(
        &self,
        decision: &ReorderDecision,
        simulation: &ReorderSimulation,
    ) -> Result<LlmReasoning, LlmReasoningError> {
        Ok(LlmReasoning {
            summary: format!(
                "Recommend {} units for SKU {} at {} because deterministic policy found {:?} risk and simulation selected the best capacity-safe option.",
                simulation.selected_quantity,
                decision.sku_id,
                decision.location_code,
                decision.risk_level
            ),
            key_points: decision.reason_codes.clone(),
            confidence: 0.85,
            tools_used: Vec::new(),
            tool_calls: Vec::new(),
        })
    }
}

pub struct ProductionLlmReasoner {
    enabled: bool,
}

impl ProductionLlmReasoner {
    pub fn new(api_key: Option<&str>) -> Self {
        Self {
            enabled: api_key.is_some_and(|value| !value.trim().is_empty()),
        }
    }
}

impl LlmReasoner for ProductionLlmReasoner {
    fn explain(
        &self,
        _decision: &ReorderDecision,
        _simulation: &ReorderSimulation,
    ) -> Result<LlmReasoning, LlmReasoningError> {
        if !self.enabled {
            return Err(LlmReasoningError::Unavailable);
        }

        Err(LlmReasoningError::Unavailable)
    }
}

pub fn parse_llm_reasoning_json(value: &str) -> Result<LlmReasoning, LlmReasoningError> {
    let reasoning: LlmReasoning = serde_json::from_str(value)
        .map_err(|error| LlmReasoningError::Malformed(error.to_string()))?;
    if reasoning.summary.trim().is_empty()
        || reasoning.key_points.is_empty()
        || !(0.0..=1.0).contains(&reasoning.confidence)
    {
        return Err(LlmReasoningError::Malformed(
            "summary, key_points, and confidence are required".to_owned(),
        ));
    }
    Ok(reasoning)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProposalStatus {
    Proposed,
    Rejected,
    Approved,
    Executing,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReorderProposal {
    pub proposal_id: Uuid,
    pub decision_id: Uuid,
    pub sku_id: i32,
    pub location_code: String,
    pub quantity: i32,
    pub status: ProposalStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub reasoning: LlmReasoning,
}

impl ReorderProposal {
    pub fn from_decision(
        decision: &ReorderDecision,
        quantity: i32,
        reasoning: LlmReasoning,
    ) -> Self {
        let identity = format!(
            "{}:{}:{}:{}",
            decision.decision_id, decision.sku_id, decision.location_code, quantity
        );
        Self {
            proposal_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
            decision_id: decision.decision_id,
            sku_id: decision.sku_id,
            location_code: decision.location_code.clone(),
            quantity,
            status: ProposalStatus::Proposed,
            created_at: decision.created_at,
            updated_at: decision.created_at,
            correlation_id: decision.decision_id,
            reasoning,
        }
    }

    pub fn key(&self) -> SkuLocation {
        SkuLocation::new(self.sku_id, &self.location_code)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyOutcome {
    AutoApproved,
    RequiresHumanApproval,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyDecision {
    pub policy_decision_id: Uuid,
    pub proposal_id: Uuid,
    pub outcome: PolicyOutcome,
    pub reason_codes: Vec<String>,
    pub policy_version: String,
    pub decided_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct GovernancePolicyConfig {
    pub policy_version: String,
    pub allow_medium_auto_execution: bool,
    pub allow_high_auto_execution: bool,
    pub allow_critical_auto_execution: bool,
}

impl Default for GovernancePolicyConfig {
    fn default() -> Self {
        Self {
            policy_version: "replenishment-governance-v1".to_owned(),
            allow_medium_auto_execution: false,
            allow_high_auto_execution: false,
            allow_critical_auto_execution: false,
        }
    }
}

pub struct PolicyGate {
    config: GovernancePolicyConfig,
}

impl PolicyGate {
    pub fn new(config: GovernancePolicyConfig) -> Self {
        Self { config }
    }

    pub fn decide(
        &self,
        proposal: &ReorderProposal,
        decision: &ReorderDecision,
        data_status: DataFreshnessStatus,
    ) -> PolicyDecision {
        let mut reason_codes = Vec::new();
        let outcome = if !matches!(
            data_status,
            DataFreshnessStatus::Final | DataFreshnessStatus::Provisional
        ) {
            reason_codes.push("DATA_NOT_EXECUTION_SAFE".to_owned());
            PolicyOutcome::RequiresHumanApproval
        } else if proposal.quantity <= 0 {
            reason_codes.push("NO_REORDER_QUANTITY".to_owned());
            PolicyOutcome::Rejected
        } else {
            match decision.risk_level {
                RiskLevel::Low => {
                    reason_codes.push("LOW_RISK_AUTO_POLICY".to_owned());
                    PolicyOutcome::AutoApproved
                }
                RiskLevel::Medium if self.config.allow_medium_auto_execution => {
                    reason_codes.push("MEDIUM_RISK_AUTO_POLICY_ENABLED".to_owned());
                    PolicyOutcome::AutoApproved
                }
                RiskLevel::Medium => {
                    reason_codes.push("MEDIUM_RISK_REQUIRES_APPROVAL".to_owned());
                    PolicyOutcome::RequiresHumanApproval
                }
                RiskLevel::High => {
                    if self.config.allow_high_auto_execution {
                        reason_codes.push("HIGH_RISK_AUTO_POLICY_ENABLED".to_owned());
                        PolicyOutcome::AutoApproved
                    } else {
                        reason_codes.push("HIGH_RISK_REQUIRES_APPROVAL".to_owned());
                        PolicyOutcome::RequiresHumanApproval
                    }
                }
                RiskLevel::Critical => {
                    if self.config.allow_critical_auto_execution {
                        reason_codes.push("CRITICAL_RISK_AUTO_POLICY_ENABLED".to_owned());
                        PolicyOutcome::AutoApproved
                    } else {
                        reason_codes.push("CRITICAL_RISK_REQUIRES_APPROVAL".to_owned());
                        PolicyOutcome::RequiresHumanApproval
                    }
                }
            }
        };
        let identity = format!(
            "{}:{}:{}",
            proposal.proposal_id,
            self.config.policy_version,
            reason_codes.join(",")
        );

        PolicyDecision {
            policy_decision_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
            proposal_id: proposal.proposal_id,
            outcome,
            reason_codes,
            policy_version: self.config.policy_version.clone(),
            decided_at: decision.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionAttempt {
    pub attempt_id: Uuid,
    pub proposal_id: Uuid,
    pub operation_id: Uuid,
    pub status: ProposalStatus,
    pub message: String,
    pub attempted_at: DateTime<Utc>,
}

pub struct ProposalExecutor;

impl ProposalExecutor {
    pub fn validate_start(
        proposal: &ReorderProposal,
        policy: &PolicyDecision,
        current_balance: &InventoryBalanceState,
    ) -> Result<ExecutionAttempt, ExecutionAttempt> {
        let attempted_at = Utc::now();
        let failure = |message: String| ExecutionAttempt {
            attempt_id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("{}:{message}", proposal.proposal_id).as_bytes(),
            ),
            proposal_id: proposal.proposal_id,
            operation_id: proposal.proposal_id,
            status: ProposalStatus::Failed,
            message,
            attempted_at,
        };

        if policy.outcome != PolicyOutcome::AutoApproved {
            return Err(failure(
                "policy does not allow automatic execution".to_owned(),
            ));
        }
        if proposal.status != ProposalStatus::Approved
            && proposal.status != ProposalStatus::Proposed
        {
            return Err(failure("proposal is not executable".to_owned()));
        }
        if proposal.key() != current_balance.sku_location() {
            return Err(failure(
                "proposal does not match current inventory key".to_owned(),
            ));
        }
        let capacity = current_balance.max_stock - current_balance.on_hand;
        if proposal.quantity <= 0 || proposal.quantity > capacity {
            return Err(failure(
                "proposal quantity exceeds current capacity".to_owned(),
            ));
        }

        Ok(ExecutionAttempt {
            attempt_id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("{}:{}", proposal.proposal_id, current_balance.version).as_bytes(),
            ),
            proposal_id: proposal.proposal_id,
            operation_id: proposal.proposal_id,
            status: ProposalStatus::Executing,
            message: "execution validated".to_owned(),
            attempted_at,
        })
    }
}

pub struct ReplenishmentAgent {
    config: ReplenishmentPolicyConfig,
    mode: AgentMode,
}

impl ReplenishmentAgent {
    pub fn new(config: ReplenishmentPolicyConfig, mode: AgentMode) -> Self {
        Self { config, mode }
    }

    pub fn evaluate(
        &self,
        features: &SkuLocationFeatures,
        balance: &InventoryBalanceState,
        incoming_replenishment: i32,
    ) -> ReorderDecision {
        let forecast = features.forecast.forecast_units_per_day.max(0.0);
        let inventory_position = balance.on_hand - balance.reserved + incoming_replenishment;
        let learned_safety = self.config.service_factor
            * features.forecast.historical_stddev_units_per_day
            * self.config.lead_time_days.sqrt();
        let safety_stock = (learned_safety.ceil() as i32).max(balance.safety_stock);
        let horizon_days =
            (self.config.lead_time_days + self.config.review_period_days).ceil() as usize;
        let horizon_forecast = features
            .forecast_horizon_days
            .iter()
            .take(horizon_days.max(1))
            .cloned()
            .collect::<Vec<_>>();
        let horizon_demand = if horizon_forecast.is_empty() {
            forecast * (self.config.lead_time_days + self.config.review_period_days)
        } else {
            horizon_forecast
                .iter()
                .map(|point| point.forecast_units)
                .sum()
        };
        let uncapped_target = horizon_demand.ceil() as i32 + safety_stock;
        let target_inventory = uncapped_target.clamp(0, balance.max_stock);
        let capacity = (balance.max_stock - balance.on_hand - incoming_replenishment).max(0);
        let recommended_quantity = (target_inventory - inventory_position).max(0).min(capacity);

        let stockout_hours = if forecast > 0.0 {
            Some(balance.available.max(0) as f64 / forecast * 24.0)
        } else {
            None
        };
        let risk_level = match stockout_hours {
            Some(hours) if hours <= self.config.critical_risk_hours => RiskLevel::Critical,
            Some(hours) if hours <= self.config.high_risk_hours => RiskLevel::High,
            Some(hours) if hours <= self.config.medium_risk_hours => RiskLevel::Medium,
            _ => RiskLevel::Low,
        };
        let estimated_stockout_at = stockout_hours.map(|hours| {
            features.computed_at + Duration::milliseconds((hours * 3_600_000.0) as i64)
        });
        let mut reasons = Vec::new();
        if features.forecast.spike_detected {
            reasons.push("DEMAND_SPIKE".to_owned());
        }
        if matches!(risk_level, RiskLevel::High | RiskLevel::Critical) {
            reasons.push("STOCKOUT_BEFORE_OR_NEAR_LEAD_TIME".to_owned());
        }
        if recommended_quantity > 0 {
            reasons.push("INVENTORY_POSITION_BELOW_ADAPTIVE_TARGET".to_owned());
        } else {
            reasons.push("INVENTORY_POSITION_SUFFICIENT".to_owned());
        }

        let decision_identity = format!(
            "{}:{}:{}:{}:{}:{}",
            balance.sku_id,
            balance.location_code,
            balance.version,
            features.computed_at.timestamp_millis(),
            recommended_quantity,
            self.config.model_version
        );

        ReorderDecision {
            decision_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, decision_identity.as_bytes()),
            sku_id: balance.sku_id,
            location_code: balance.location_code.clone(),
            created_at: features.computed_at,
            forecast_units_per_day: forecast,
            baseline_units_per_day: features.forecast.baseline_units_per_day,
            recent_units_per_day: features.forecast.recent_units_per_day,
            spike_score: features.forecast.spike_score,
            risk_level,
            estimated_stockout_at,
            inventory_position,
            safety_stock,
            target_inventory,
            recommended_quantity,
            horizon_demand_units: horizon_demand,
            horizon_forecast,
            reason_codes: reasons,
            model_version: self.config.model_version.clone(),
            mode: self.mode,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use data_platform_common::{InventoryBalanceState, SkuLocation};
    use feature_engine::{AdaptiveForecast, SkuLocationFeatures};

    use super::*;

    fn input(
        forecast: f64,
        available: i32,
        max_stock: i32,
    ) -> (SkuLocationFeatures, InventoryBalanceState) {
        let now = Utc.with_ymd_and_hms(2026, 9, 15, 12, 0, 0).unwrap();
        let balance = InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: available,
            reserved: 0,
            available,
            safety_stock: 10,
            reorder_point: 20,
            max_stock,
            version: 7,
            updated_at: now,
        };
        let features = SkuLocationFeatures {
            key: SkuLocation::new(42, "NCR"),
            computed_at: now,
            on_hand: available,
            reserved: 0,
            available,
            balance_version: 7,
            reserve_units_5m: 0,
            sale_units_5m: 0,
            sale_units_15m: 0,
            sale_units_1h: 0,
            sale_velocity_5m: forecast,
            sale_velocity_15m: forecast,
            sale_velocity_1h: forecast,
            daily_sales: 0,
            forecast: AdaptiveForecast {
                baseline_units_per_day: 5.0,
                recent_units_per_day: forecast,
                forecast_units_per_day: forecast,
                historical_stddev_units_per_day: 3.0,
                recent_vs_baseline_ratio: forecast / 5.0,
                spike_score: (forecast - 5.0).max(0.0) / 3.0,
                spike_detected: forecast >= 10.0,
            },
            forecast_horizon_days: vec![
                DailyForecastPoint {
                    date: "2026-09-15".to_owned(),
                    weekday: "TUE".to_owned(),
                    forecast_units: forecast,
                    baseline_units: 5.0,
                    recent_units: forecast,
                    spike_detected: forecast >= 10.0,
                },
                DailyForecastPoint {
                    date: "2026-09-16".to_owned(),
                    weekday: "WED".to_owned(),
                    forecast_units: forecast,
                    baseline_units: 5.0,
                    recent_units: forecast,
                    spike_detected: forecast >= 10.0,
                },
                DailyForecastPoint {
                    date: "2026-09-17".to_owned(),
                    weekday: "THU".to_owned(),
                    forecast_units: forecast,
                    baseline_units: 5.0,
                    recent_units: forecast,
                    spike_detected: forecast >= 10.0,
                },
            ],
        };
        (features, balance)
    }

    #[test]
    fn high_demand_shortens_stockout_eta_and_creates_reorder() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (features, balance) = input(40.0, 50, 200);
        let decision = agent.evaluate(&features, &balance, 0);
        assert_eq!(decision.risk_level, RiskLevel::High);
        assert!(decision.recommended_quantity > 0);
        assert!(decision.reason_codes.contains(&"DEMAND_SPIKE".to_owned()));
    }

    #[test]
    fn sufficient_position_does_not_reorder() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Observe);
        let (features, balance) = input(5.0, 100, 200);
        let decision = agent.evaluate(&features, &balance, 0);
        assert_eq!(decision.recommended_quantity, 0);
        assert_eq!(decision.risk_level, RiskLevel::Low);
    }

    #[test]
    fn sufficient_restock_coverage_lowers_stockout_risk() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (before_features, before_balance) = input(40.0, 5, 250);
        let before = agent.evaluate(&before_features, &before_balance, 0);
        let (after_features, after_balance) = input(40.0, 200, 250);
        let after = agent.evaluate(&after_features, &after_balance, 0);

        assert_eq!(before.risk_level, RiskLevel::Critical);
        assert_eq!(after.risk_level, RiskLevel::Low);
        assert!(after.estimated_stockout_at > before.estimated_stockout_at);
    }

    #[test]
    fn reorder_is_capped_by_operational_max_stock() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (features, balance) = input(500.0, 190, 200);
        let decision = agent.evaluate(&features, &balance, 0);
        assert_eq!(decision.recommended_quantity, 10);
        assert!(balance.on_hand + decision.recommended_quantity <= balance.max_stock);
    }

    #[test]
    fn identical_evaluation_has_stable_decision_identity() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::AutoDemo);
        let (features, balance) = input(40.0, 50, 200);
        let first = agent.evaluate(&features, &balance, 0);
        let duplicate = agent.evaluate(&features, &balance, 0);
        assert_eq!(first.decision_id, duplicate.decision_id);
    }

    #[test]
    fn reorder_uses_day_by_day_forecast_horizon() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (mut features, balance) = input(10.0, 15, 250);
        features.forecast_horizon_days = vec![
            DailyForecastPoint {
                date: "2026-09-11".to_owned(),
                weekday: "FRI".to_owned(),
                forecast_units: 10.0,
                baseline_units: 10.0,
                recent_units: 0.0,
                spike_detected: false,
            },
            DailyForecastPoint {
                date: "2026-09-12".to_owned(),
                weekday: "SAT".to_owned(),
                forecast_units: 12.0,
                baseline_units: 12.0,
                recent_units: 0.0,
                spike_detected: false,
            },
            DailyForecastPoint {
                date: "2026-09-13".to_owned(),
                weekday: "SUN".to_owned(),
                forecast_units: 90.0,
                baseline_units: 90.0,
                recent_units: 0.0,
                spike_detected: false,
            },
        ];

        let decision = agent.evaluate(&features, &balance, 0);

        assert_eq!(decision.horizon_demand_units, 112.0);
        assert_eq!(decision.horizon_forecast[2].weekday, "SUN");
        assert!(decision.recommended_quantity >= 100);
    }

    #[test]
    fn simulation_compares_candidates_and_respects_max_stock_capacity() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (features, balance) = input(40.0, 10, 100);
        let decision = agent.evaluate(&features, &balance, 0);
        let simulation =
            SimulationEngine::compare_reorder_quantities(&decision, &balance, 0, &[20, 81, 150]);

        assert_eq!(simulation.alternatives.len(), 3);
        assert_eq!(simulation.selected_quantity, 90);
        assert_eq!(simulation.alternatives[2].capacity_violation_units, 60);
    }

    #[test]
    fn llm_reasoning_parser_rejects_malformed_or_unsafe_output() {
        assert!(parse_llm_reasoning_json("not-json").is_err());
        assert!(
            parse_llm_reasoning_json(r#"{"summary":"","key_points":[],"confidence":2.0}"#).is_err()
        );

        let parsed = parse_llm_reasoning_json(
            r#"{"summary":"Use deterministic simulation result.","key_points":["LOW_STOCK"],"confidence":0.8}"#,
        )
        .unwrap();
        assert_eq!(parsed.confidence, 0.8);
    }

    #[test]
    fn policy_gate_blocks_stale_data_and_high_risk_auto_execution() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (features, balance) = input(40.0, 10, 200);
        let decision = agent.evaluate(&features, &balance, 0);
        let simulation =
            SimulationEngine::compare_reorder_quantities(&decision, &balance, 0, &[20, 81, 150]);
        let reasoning = MockLlmReasoner.explain(&decision, &simulation).unwrap();
        let proposal =
            ReorderProposal::from_decision(&decision, simulation.selected_quantity, reasoning);
        let gate = PolicyGate::new(Default::default());

        let stale = gate.decide(&proposal, &decision, DataFreshnessStatus::Stale);
        assert_eq!(stale.outcome, PolicyOutcome::RequiresHumanApproval);

        let fresh = gate.decide(&proposal, &decision, DataFreshnessStatus::Final);
        assert_eq!(fresh.outcome, PolicyOutcome::RequiresHumanApproval);
        assert!(
            fresh
                .reason_codes
                .contains(&"HIGH_RISK_REQUIRES_APPROVAL".to_owned())
                || fresh
                    .reason_codes
                    .contains(&"CRITICAL_RISK_REQUIRES_APPROVAL".to_owned())
        );
    }

    #[test]
    fn executor_validates_policy_identity_and_capacity_before_mutation() {
        let agent = ReplenishmentAgent::new(Default::default(), AgentMode::Recommend);
        let (features, mut balance) = input(5.0, 50, 100);
        let decision = agent.evaluate(&features, &balance, 0);
        let reasoning = LlmReasoning {
            summary: "safe low-risk replenishment".to_owned(),
            key_points: vec!["LOW_RISK_AUTO_POLICY".to_owned()],
            confidence: 0.9,
            tools_used: Vec::new(),
            tool_calls: Vec::new(),
        };
        let mut proposal = ReorderProposal::from_decision(&decision, 10, reasoning);
        proposal.status = ProposalStatus::Approved;
        let policy = PolicyDecision {
            policy_decision_id: Uuid::new_v4(),
            proposal_id: proposal.proposal_id,
            outcome: PolicyOutcome::AutoApproved,
            reason_codes: vec!["LOW_RISK_AUTO_POLICY".to_owned()],
            policy_version: "test".to_owned(),
            decided_at: decision.created_at,
        };

        let attempt = ProposalExecutor::validate_start(&proposal, &policy, &balance).unwrap();
        assert_eq!(attempt.status, ProposalStatus::Executing);

        balance.location_code = "BLR".to_owned();
        let rejected = ProposalExecutor::validate_start(&proposal, &policy, &balance).unwrap_err();
        assert_eq!(rejected.status, ProposalStatus::Failed);
    }
}
