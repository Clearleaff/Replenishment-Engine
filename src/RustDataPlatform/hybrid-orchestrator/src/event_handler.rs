use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use data_platform_common::{InventoryBalanceState, InventoryMovementFact, InventoryMovementType};
use feature_engine::SkuLocationFeatures;
use futures::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueDeclareOptions},
    types::FieldTable,
};
use replenishment_agent::{
    AgentMode, DataFreshnessStatus, GovernancePolicyConfig, PolicyGate, PolicyOutcome,
    ProposalExecutor, ProposalStatus, ReorderProposal, ReplenishmentAgent,
    ReplenishmentPolicyConfig, SimulationEngine,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    llm_client::LlmFallbackReason,
    llm_tools::ToolExecutionContext,
    state::{AppState, ProposalRecord, ProposalRecordInput},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderStockConfirmedIntegrationEvent {
    #[serde(default)]
    pub event_id: Option<Uuid>,
    pub order_id: i32,
    pub sku_id: i32,
    pub location_code: String,
    pub quantity_depleted: i32,
    #[serde(default)]
    pub occurred_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub balance: Option<InventoryBalancePayload>,
}

impl OrderStockConfirmedIntegrationEvent {
    fn validate(&self) -> Result<()> {
        if self.order_id <= 0 {
            bail!("order_id must be positive");
        }
        if self.sku_id <= 0 {
            bail!("sku_id must be positive");
        }
        if self.location_code.trim().is_empty() {
            bail!("location_code is required");
        }
        if self.quantity_depleted <= 0 {
            bail!("quantity_depleted must be positive");
        }
        Ok(())
    }

    fn source_event_key(&self) -> String {
        self.event_id.map_or_else(
            || {
                format!(
                    "order-stock-confirmed:{}:{}:{}:{}",
                    self.order_id,
                    self.sku_id,
                    self.location_code.trim().to_ascii_uppercase(),
                    self.quantity_depleted
                )
            },
            |event_id| event_id.to_string(),
        )
    }

    fn source_event_id(&self) -> Uuid {
        self.event_id.unwrap_or_else(|| {
            Uuid::new_v5(&Uuid::NAMESPACE_OID, self.source_event_key().as_bytes())
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InventoryBalancePayload {
    pub on_hand: i32,
    pub reserved: i32,
    pub safety_stock: i32,
    pub reorder_point: i32,
    pub max_stock: i32,
    pub version: i64,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub authoritative: bool,
}

impl InventoryBalancePayload {
    fn to_state(&self, event: &OrderStockConfirmedIntegrationEvent) -> InventoryBalanceState {
        InventoryBalanceState {
            sku_id: event.sku_id,
            location_code: event.location_code.trim().to_ascii_uppercase(),
            on_hand: self.on_hand,
            reserved: self.reserved,
            available: self.on_hand - self.reserved,
            safety_stock: self.safety_stock,
            reorder_point: self.reorder_point,
            max_stock: self.max_stock,
            version: self.version,
            updated_at: self.updated_at.unwrap_or_else(Utc::now),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandleOutcome {
    NoAction,
    ProposalStored,
    AutoApprovedPrepared,
    Rejected,
    Duplicate,
}

#[derive(Debug)]
enum HandlerError {
    Poison(anyhow::Error),
    Transient(anyhow::Error),
}

impl HandlerError {
    fn poison(error: impl Into<anyhow::Error>) -> Self {
        Self::Poison(error.into())
    }

    fn transient(error: impl Into<anyhow::Error>) -> Self {
        Self::Transient(error.into())
    }

    fn requeue(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

pub async fn start_consumer(state: AppState) -> Result<()> {
    loop {
        state.set_amqp_connected(false).await;
        let conn = connect_with_retry(&state).await?;
        state.set_amqp_connected(true).await;
        if let Err(error) = consume_until_disconnect(conn, state.clone()).await {
            error!(?error, "RabbitMQ consumer loop ended; reconnecting");
            state.set_amqp_connected(false).await;
            state.incr(|metrics| metrics.amqp_reconnects += 1).await;
        }
    }
}

async fn connect_with_retry(state: &AppState) -> Result<Connection> {
    let url = state.config.amqp_url.clone();
    let strategy = ExponentialBackoff::from_millis(500)
        .factor(2)
        .max_delay(state.config.amqp_max_retry_delay);
    Retry::start(strategy, move || {
        let url = url.clone();
        async move {
            Connection::connect(&url, ConnectionProperties::default())
                .await
                .map_err(|error| {
                    warn!(?error, "RabbitMQ not ready yet; retrying AMQP connection");
                    error
                })
        }
    })
    .await
    .context("connecting to RabbitMQ failed after retry")
}

async fn consume_until_disconnect(conn: Connection, state: AppState) -> Result<()> {
    let channel = conn
        .create_channel()
        .await
        .context("creating RabbitMQ channel failed")?;
    channel
        .queue_declare(
            &state.config.queue_name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declaring hybrid orchestrator queue failed")?;

    let mut consumer = channel
        .basic_consume(
            &state.config.queue_name,
            &state.config.consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("subscribing RabbitMQ consumer failed")?;

    info!(queue = %state.config.queue_name, "subscribed to RabbitMQ queue");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("RabbitMQ delivery error")?;
        state.incr(|metrics| metrics.deliveries_seen += 1).await;

        let event =
            match serde_json::from_slice::<OrderStockConfirmedIntegrationEvent>(&delivery.data) {
                Ok(event) => event,
                Err(error) => {
                    warn!(?error, "rejecting malformed order-stock-confirmed payload");
                    state.incr(|metrics| metrics.invalid_events += 1).await;
                    delivery
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: false,
                        })
                        .await?;
                    continue;
                }
            };

        match handle_event(event, state.clone()).await {
            Ok(HandleOutcome::Duplicate) => {
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Ok(outcome) => {
                info!(?outcome, "event processed");
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(error) => {
                let requeue = error.requeue();
                match &error {
                    HandlerError::Poison(inner) => {
                        warn!(?inner, "poison message rejected without requeue")
                    }
                    HandlerError::Transient(inner) => error!(
                        ?inner,
                        "transient processing error; message will be requeued"
                    ),
                }
                state.incr(|metrics| metrics.processing_errors += 1).await;
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue,
                    })
                    .await?;
            }
        }
    }

    Err(anyhow!("RabbitMQ consumer stream ended"))
}

async fn handle_event(
    event: OrderStockConfirmedIntegrationEvent,
    state: AppState,
) -> std::result::Result<HandleOutcome, HandlerError> {
    event.validate().map_err(HandlerError::poison)?;
    let event_key = event.source_event_key();

    if !state.remember_event(&event_key).await {
        state.incr(|metrics| metrics.duplicate_events += 1).await;
        info!(source_event_key = %event_key, "duplicate event skipped idempotently");
        return Ok(HandleOutcome::Duplicate);
    }

    let Some((features, balance, data_status)) = build_features(&event, &state).await? else {
        state.incr(|metrics| metrics.feature_skips += 1).await;
        warn!(source_event_key = %event_key, "event missing balance snapshot; refusing to fabricate stock state");
        return Ok(HandleOutcome::NoAction);
    };

    state.incr(|metrics| metrics.events_processed += 1).await;

    let agent = ReplenishmentAgent::new(ReplenishmentPolicyConfig::default(), AgentMode::Recommend);
    let base_decision = agent.evaluate(&features, &balance, 0);

    if !features.forecast.spike_detected && base_decision.recommended_quantity <= 0 {
        return Ok(HandleOutcome::NoAction);
    }

    let feature_state_val = state.get_feature_state(&balance.sku_location()).await;
    let feature_state_mutex = feature_state_val.map(Mutex::new);

    let ctx = ToolExecutionContext {
        warehouse: &state.warehouse,
        current_balance: &balance,
        current_features: Some(&features),
        feature_state: feature_state_mutex.as_ref(),
        base_decision: &base_decision,
        timeout: state.config.llm.tool_call_timeout,
    };

    let llm_decision = state.llm.propose_with_tools(&ctx, &balance).await;
    if let Some(reason) = llm_decision.fallback_reason {
        state
            .incr(|metrics| {
                metrics.llm_fallbacks += 1;
                if reason == LlmFallbackReason::PolicyRejected {
                    metrics.llm_policy_rejections += 1;
                }
            })
            .await;
    }

    let mut decision = base_decision;
    decision.recommended_quantity = llm_decision.proposed_quantity;
    decision.risk_level = llm_decision.risk_level;

    let candidates = candidate_quantities(&decision, &balance);
    let simulation =
        SimulationEngine::compare_reorder_quantities(&decision, &balance, 0, &candidates);

    let mut proposal = ReorderProposal::from_decision(
        &decision,
        llm_decision.proposed_quantity,
        llm_decision.reasoning,
    );

    let policy = PolicyGate::new(GovernancePolicyConfig::default()).decide(
        &proposal,
        &decision,
        data_status,
    );

    let mut message = format!(
        "policy outcome {:?}; execution mode {:?}",
        policy.outcome, state.config.execution_mode
    );
    let outcome = match policy.outcome {
        PolicyOutcome::AutoApproved => {
            proposal.status = ProposalStatus::Approved;
            state.incr(|metrics| metrics.auto_approved += 1).await;
            match ProposalExecutor::validate_start(&proposal, &policy, &balance) {
                Ok(attempt) => {
                    message = format!(
                        "auto-approved and execution preflight passed; LOG_ONLY command emission recorded for operation {}",
                        attempt.operation_id
                    );
                }
                Err(attempt) => {
                    proposal.status = ProposalStatus::Failed;
                    message = format!(
                        "auto-approval blocked by execution preflight: {}",
                        attempt.message
                    );
                }
            }
            HandleOutcome::AutoApprovedPrepared
        }
        PolicyOutcome::RequiresHumanApproval => {
            state
                .incr(|metrics| metrics.human_approval_required += 1)
                .await;
            HandleOutcome::ProposalStored
        }
        PolicyOutcome::Rejected => {
            proposal.status = ProposalStatus::Rejected;
            state.incr(|metrics| metrics.rejected_by_policy += 1).await;
            HandleOutcome::Rejected
        }
    };

    let record = ProposalRecord::from_input(ProposalRecordInput {
        proposal,
        decision,
        simulation,
        policy,
        balance,
        features,
        data_status,
        source_event_key: event_key,
        message,
        ttl: state.config.proposal_ttl,
    });

    info!(
        proposal_id = %record.proposal.proposal_id,
        sku_id = record.proposal.sku_id,
        location_code = %record.proposal.location_code,
        quantity = record.proposal.quantity,
        status = ?record.status,
        data_status = ?record.data_status,
        "proposal recorded"
    );
    state.insert_proposal(record).await;
    state.incr(|metrics| metrics.proposals_created += 1).await;

    Ok(outcome)
}

async fn build_features(
    event: &OrderStockConfirmedIntegrationEvent,
    state: &AppState,
) -> std::result::Result<
    Option<(
        SkuLocationFeatures,
        InventoryBalanceState,
        DataFreshnessStatus,
    )>,
    HandlerError,
> {
    let Some(payload) = &event.balance else {
        return Ok(None);
    };

    let balance = payload.to_state(event);
    if !balance.invariants_hold() {
        return Err(HandlerError::poison(anyhow!(
            "inventory balance invariants failed for sku={} location={}",
            balance.sku_id,
            balance.location_code
        )));
    }

    let occurred_at = event.occurred_at.unwrap_or_else(Utc::now);
    let movement = InventoryMovementFact {
        movement_id: Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:sale", event.source_event_key()).as_bytes(),
        ),
        source_event_id: event.source_event_id(),
        sku_id: event.sku_id,
        location_code: event.location_code.trim().to_ascii_uppercase(),
        order_id: Some(event.order_id),
        movement_type: InventoryMovementType::Sale,
        quantity: event.quantity_depleted,
        occurred_at,
        recorded_at: Utc::now(),
        resulting_balance_version: balance.version,
        reason: Some("ORDER_STOCK_CONFIRMED".to_owned()),
    };

    let features = state
        .update_features(balance.clone(), movement, Utc::now())
        .await
        .ok_or_else(|| {
            HandlerError::transient(anyhow!("feature calculation returned no balance state"))
        })?;
    let data_status = if payload.authoritative {
        DataFreshnessStatus::Final
    } else {
        DataFreshnessStatus::Unreconciled
    };

    Ok(Some((features, balance, data_status)))
}

fn candidate_quantities(
    decision: &replenishment_agent::ReorderDecision,
    balance: &InventoryBalanceState,
) -> Vec<i32> {
    let capacity = (balance.max_stock - balance.on_hand).max(0);
    let recommended = decision.recommended_quantity.max(0);
    let mut candidates = vec![
        0,
        recommended / 2,
        recommended,
        ((recommended as f64) * 1.25).ceil() as i32,
        capacity,
    ];
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::OrchestratorConfig, state::AppState};

    fn event_with_balance(authoritative: bool) -> OrderStockConfirmedIntegrationEvent {
        OrderStockConfirmedIntegrationEvent {
            event_id: Some(Uuid::new_v4()),
            order_id: 1001,
            sku_id: 42,
            location_code: "ncr".to_owned(),
            quantity_depleted: 8,
            occurred_at: Some(Utc::now()),
            balance: Some(InventoryBalancePayload {
                on_hand: 12,
                reserved: 0,
                safety_stock: 10,
                reorder_point: 30,
                max_stock: 100,
                version: 9,
                updated_at: Some(Utc::now()),
                authoritative,
            }),
        }
    }

    #[test]
    fn validation_rejects_non_positive_quantity() {
        let mut event = event_with_balance(true);
        event.quantity_depleted = 0;
        assert!(event.validate().is_err());
    }

    #[tokio::test]
    async fn missing_balance_is_not_fabricated() {
        let state = AppState::new(OrchestratorConfig::from_env());
        let mut event = event_with_balance(true);
        event.balance = None;
        assert!(build_features(&event, &state).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn supplied_authoritative_balance_produces_final_features() {
        let state = AppState::new(OrchestratorConfig::from_env());
        let event = event_with_balance(true);
        let (_, balance, data_status) = build_features(&event, &state).await.unwrap().unwrap();
        assert_eq!(balance.location_code, "NCR");
        assert_eq!(data_status, DataFreshnessStatus::Final);
    }

    #[tokio::test]
    async fn non_authoritative_balance_forces_unreconciled_status() {
        let state = AppState::new(OrchestratorConfig::from_env());
        let event = event_with_balance(false);
        let (_, _, data_status) = build_features(&event, &state).await.unwrap().unwrap();
        assert_eq!(data_status, DataFreshnessStatus::Unreconciled);
    }
}
