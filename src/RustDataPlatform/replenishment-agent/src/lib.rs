use chrono::{DateTime, Duration, Utc};
use data_platform_common::InventoryBalanceState;
use feature_engine::SkuLocationFeatures;
use serde::{Deserialize, Serialize};
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
    pub reason_codes: Vec<String>,
    pub model_version: String,
    pub mode: AgentMode,
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
        let horizon_demand =
            forecast * (self.config.lead_time_days + self.config.review_period_days);
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
}
