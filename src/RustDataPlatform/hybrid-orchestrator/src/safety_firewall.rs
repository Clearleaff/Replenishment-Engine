use data_platform_common::InventoryBalanceState;
use replenishment_agent::LlmReasoning;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmDecisionResult {
    pub action: String,
    pub reorder_quantity: i32,
    pub urgency: String,
    pub risk_level: String,
    pub reasoning: LlmReasoning,
}

#[derive(Debug, Clone)]
pub struct SafetyFirewall {
    pub max_order_quantity: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FirewallOutcome {
    Passed(LlmDecisionResult),
    Clamped {
        original_quantity: i32,
        clamped_quantity: i32,
        reason: String,
        decision: LlmDecisionResult,
    },
    Rejected {
        reason: String,
    },
}

impl SafetyFirewall {
    pub fn new(max_order_quantity: i32) -> Self {
        Self { max_order_quantity }
    }

    pub fn validate(
        &self,
        decision: LlmDecisionResult,
        balance: &InventoryBalanceState,
    ) -> FirewallOutcome {
        let raw_action = decision.action.trim().to_ascii_uppercase();
        let action = if raw_action.contains("REORDER") {
            "REORDER".to_owned()
        } else if raw_action.contains("WAIT") {
            "WAIT".to_owned()
        } else if raw_action.contains("REVIEW") {
            "REVIEW".to_owned()
        } else {
            return FirewallOutcome::Rejected {
                reason: format!(
                    "invalid action '{}'; must be REORDER, WAIT, or REVIEW",
                    decision.action
                ),
            };
        };

        if !(0.0..=1.0).contains(&decision.reasoning.confidence) {
            return FirewallOutcome::Rejected {
                reason: format!(
                    "confidence {} out of [0.0, 1.0]",
                    decision.reasoning.confidence
                ),
            };
        }

        if decision.reasoning.summary.trim().is_empty() {
            return FirewallOutcome::Rejected {
                reason: "summary is empty".to_owned(),
            };
        }
        if decision.reasoning.key_points.is_empty() {
            return FirewallOutcome::Rejected {
                reason: "key_points array is empty".to_owned(),
            };
        }

        let mut current_decision = decision;
        current_decision.action = action;

        if current_decision.action != "REORDER" && current_decision.reorder_quantity > 0 {
            let original = current_decision.reorder_quantity;
            current_decision.reorder_quantity = 0;
            return FirewallOutcome::Clamped {
                original_quantity: original,
                clamped_quantity: 0,
                reason: format!(
                    "action is {} but proposed quantity was {}; clamped to 0",
                    current_decision.action, original
                ),
                decision: current_decision,
            };
        }

        if current_decision.reorder_quantity < 0 {
            let original = current_decision.reorder_quantity;
            current_decision.reorder_quantity = 0;
            return FirewallOutcome::Clamped {
                original_quantity: original,
                clamped_quantity: 0,
                reason: format!("negative quantity {} clamped to 0", original),
                decision: current_decision,
            };
        }

        let capacity = (balance.max_stock - balance.on_hand).max(0);
        let mut clamped = false;
        let original_qty = current_decision.reorder_quantity;
        let mut clamp_reason = String::new();

        if current_decision.reorder_quantity > capacity {
            current_decision.reorder_quantity = capacity;
            clamped = true;
            clamp_reason = format!(
                "quantity {} exceeded available capacity {} (max_stock {} - on_hand {}); clamped to capacity",
                original_qty, capacity, balance.max_stock, balance.on_hand
            );
        }

        if current_decision.reorder_quantity > self.max_order_quantity {
            current_decision.reorder_quantity = self.max_order_quantity;
            clamped = true;
            clamp_reason = format!(
                "quantity {} exceeded max order budget limit {}; clamped",
                original_qty, self.max_order_quantity
            );
        }

        if clamped {
            warn!(
                sku_id = balance.sku_id,
                location = %balance.location_code,
                original = original_qty,
                clamped_to = current_decision.reorder_quantity,
                reason = %clamp_reason,
                "safety firewall clamped order quantity"
            );
            FirewallOutcome::Clamped {
                original_quantity: original_qty,
                clamped_quantity: current_decision.reorder_quantity,
                reason: clamp_reason,
                decision: current_decision,
            }
        } else {
            FirewallOutcome::Passed(current_decision)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_balance(on_hand: i32, max_stock: i32) -> InventoryBalanceState {
        InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand,
            reserved: 0,
            available: on_hand,
            safety_stock: 10,
            reorder_point: 20,
            max_stock,
            version: 1,
            updated_at: Utc::now(),
        }
    }

    fn sample_decision(action: &str, qty: i32, conf: f64) -> LlmDecisionResult {
        LlmDecisionResult {
            action: action.to_owned(),
            reorder_quantity: qty,
            urgency: "HIGH".to_owned(),
            risk_level: "HIGH".to_owned(),
            reasoning: LlmReasoning {
                summary: "Order required due to depletion".to_owned(),
                key_points: vec!["DEPLETION".to_owned()],
                confidence: conf,
                tools_used: vec!["get_inventory_snapshot".to_owned()],
                tool_calls: Vec::new(),
            },
        }
    }

    #[test]
    fn passes_valid_decision_within_bounds() {
        let firewall = SafetyFirewall::new(100);
        let balance = sample_balance(20, 100);
        let decision = sample_decision("REORDER", 30, 0.9);

        let outcome = firewall.validate(decision.clone(), &balance);
        assert_eq!(outcome, FirewallOutcome::Passed(decision));
    }

    #[test]
    fn clamps_quantity_exceeding_capacity() {
        let firewall = SafetyFirewall::new(500);
        let balance = sample_balance(80, 100); // capacity = 20
        let decision = sample_decision("REORDER", 50, 0.9);

        let outcome = firewall.validate(decision, &balance);
        match outcome {
            FirewallOutcome::Clamped {
                clamped_quantity, ..
            } => {
                assert_eq!(clamped_quantity, 20);
            }
            _ => panic!("expected clamped outcome"),
        }
    }

    #[test]
    fn clamps_quantity_exceeding_budget_limit() {
        let firewall = SafetyFirewall::new(50);
        let balance = sample_balance(10, 200); // capacity = 190
        let decision = sample_decision("REORDER", 100, 0.9);

        let outcome = firewall.validate(decision, &balance);
        match outcome {
            FirewallOutcome::Clamped {
                clamped_quantity, ..
            } => {
                assert_eq!(clamped_quantity, 50);
            }
            _ => panic!("expected clamped outcome"),
        }
    }

    #[test]
    fn rejects_invalid_action() {
        let firewall = SafetyFirewall::new(100);
        let balance = sample_balance(20, 100);
        let decision = sample_decision("PURCHASE_NOW", 30, 0.9);

        assert!(matches!(
            firewall.validate(decision, &balance),
            FirewallOutcome::Rejected { .. }
        ));
    }

    #[test]
    fn rejects_out_of_bounds_confidence() {
        let firewall = SafetyFirewall::new(100);
        let balance = sample_balance(20, 100);
        let decision = sample_decision("REORDER", 30, 1.5);

        assert!(matches!(
            firewall.validate(decision, &balance),
            FirewallOutcome::Rejected { .. }
        ));
    }
}
