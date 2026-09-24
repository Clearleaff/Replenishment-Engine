use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use data_platform_common::{InventoryBalanceState, InventoryMovementFact, InventoryMovementType};
use feature_engine::SkuLocationFeatures;
use futures::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
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
pub enum HandleOutcome {
    NoAction,
    ProposalStored,
    AutoApprovedPrepared,
    Rejected,
    Duplicate,
}

#[derive(Debug)]
pub enum HandlerError {
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
    let retry_strategy = ExponentialBackoff::from_millis(250)
        .max_delay(state.config.amqp_max_retry_delay)
        .take(10);

    Retry::start(retry_strategy, || async {
        info!("connecting to RabbitMQ");
        Connection::connect(&url, ConnectionProperties::default()).await
    })
    .await
    .context("connecting to RabbitMQ after retries failed")
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

    channel
        .basic_qos(state.config.amqp_prefetch, BasicQosOptions::default())
        .await
        .context("setting AMQP basic_qos prefetch failed")?;

    let mut consumer = channel
        .basic_consume(
            &state.config.queue_name,
            &state.config.consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("subscribing RabbitMQ consumer failed")?;

    info!(
        queue = %state.config.queue_name,
        prefetch = state.config.amqp_prefetch,
        "subscribed to RabbitMQ queue with basic_qos"
    );

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("RabbitMQ delivery error")?;
        state.incr(|metrics| metrics.deliveries_seen += 1).await;
        let task_state = state.clone();

        let handle = tokio::spawn(async move {
            process_delivery(delivery, task_state).await;
        });

        tokio::spawn(async move {
            if let Err(join_err) = handle.await {
                if join_err.is_panic() {
                    warn!(?join_err, "spawned per-delivery task panicked");
                }
            }
        });
    }

    Err(anyhow!("RabbitMQ consumer stream ended"))
}

async fn process_delivery(delivery: lapin::message::Delivery, state: AppState) {
    let event = match serde_json::from_slice::<OrderStockConfirmedIntegrationEvent>(&delivery.data) {
        Ok(event) => event,
        Err(error) => {
            warn!(?error, "rejecting malformed order-stock-confirmed payload");
            state.incr(|metrics| metrics.invalid_events += 1).await;
            if let Err(e) = delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: false,
                })
                .await
            {
                warn!(?e, "failed to NACK malformed payload");
            }
            return;
        }
    };

    match handle_event(event, state.clone()).await {
        Ok(HandleOutcome::Duplicate) => {
            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                warn!(?e, "failed to ACK duplicate event delivery");
            }
        }
        Ok(outcome) => {
            info!(?outcome, "event processed");
            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                warn!(?e, "failed to ACK processed delivery");
            }
        }
        Err(error) => {
            let requeue = error.requeue();
            match &error {
                HandlerError::Poison(inner) => {
                    warn!(?inner, "poison message rejected without requeue");
                }
                HandlerError::Transient(inner) => {
                    error!(?inner, "transient processing error; message will be requeued");
                }
            }
            state.incr(|metrics| metrics.processing_errors += 1).await;
            if let Err(e) = delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue,
                })
                .await
            {
                warn!(?e, "failed to NACK failed delivery");
            }
        }
    }
}

pub async fn handle_event(
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

    let Some((features, balance, data_status)) =
        build_features(&event, &state).await.map_err(HandlerError::transient)? else {
        state.incr(|metrics| metrics.feature_skips += 1).await;
        warn!(source_event_key = %event_key, "event missing balance snapshot; refusing to fabricate stock state");
        return Ok(HandleOutcome::NoAction);
    };

    state.incr(|metrics| metrics.events_processed += 1).await;

    // Trigger filter: boolean routing gate only (no reorder quantity logic, no risk scoring, no auto-approval path)
    let should_invoke_llm = features.forecast.spike_detected
        || (balance.on_hand <= (balance.reorder_point as f64 * 1.2) as i32);

    if !should_invoke_llm {
        state.incr(|metrics| metrics.llm_filter_skips += 1).await;
        return Ok(HandleOutcome::NoAction);
    }

    // Only events that passed the trigger filter reach the LLM evaluation path
    // Rate Limiter: proactive token-bucket admission gate sits between trigger filter and semaphore
    if state.config.llm.provider != crate::config::LlmProvider::Disabled {
        if let Err(crate::rate_limiter::RateLimiterError::QueueFull) =
            state.rate_limiter.acquire().await
        {
            warn!(
                sku_id = event.sku_id,
                location = %event.location_code,
                "LLM rate limiter queue is full; gracefully dropping event to protect memory and prevent 429 storms"
            );
            state
                .incr(|metrics| metrics.llm_queue_overflow_drops += 1)
                .await;
            return Ok(HandleOutcome::NoAction);
        }
    }

    // Only events that passed the trigger filter and rate limiter reach the LLM evaluation path
    let agent = ReplenishmentAgent::new(ReplenishmentPolicyConfig::default(), AgentMode::Recommend);
    let base_decision = agent.evaluate(&features, &balance, 0);

    let feature_state_val = state.get_feature_state(&balance.sku_location()).await;
    let feature_state_mutex = feature_state_val.map(Mutex::new);

    let ctx = ToolExecutionContext {
        warehouse: &state.warehouse,
        current_balance: &balance,
        current_features: Some(&features),
        feature_state: feature_state_mutex.as_ref(),
        base_decision: &base_decision,
        timeout: state.config.llm.tool_call_timeout,
        macro_cache: Some(&state.macro_cache),
    };

    // LLM_MAX_CONCURRENT semaphore permit acquired strictly around propose_with_tools call
    let llm_decision = {
        let _permit = state.llm_semaphore.acquire().await.map_err(|e| {
            HandlerError::transient(anyhow!("LLM concurrency semaphore closed: {e}"))
        })?;
        state.llm.propose_with_tools(&ctx, &balance).await
    };
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
    state.insert_proposal(record).await;
    state.incr(|metrics| metrics.proposals_created += 1).await;

    Ok(outcome)
}

async fn build_features(
    event: &OrderStockConfirmedIntegrationEvent,
    state: &AppState,
) -> Result<Option<(SkuLocationFeatures, InventoryBalanceState, DataFreshnessStatus)>> {
    let Some(balance_payload) = &event.balance else {
        return Ok(None);
    };

    let balance = balance_payload.to_state(event);
    let key = balance.sku_location();
    let occurred_at = event.occurred_at.unwrap_or_else(Utc::now);

    let movement = InventoryMovementFact {
        movement_id: event.source_event_id(),
        source_event_id: event.source_event_id(),
        sku_id: event.sku_id,
        location_code: key.location_code.clone(),
        order_id: Some(event.order_id),
        movement_type: InventoryMovementType::Sale,
        quantity: event.quantity_depleted,
        occurred_at,
        recorded_at: Utc::now(),
        resulting_balance_version: balance.version,
        reason: Some("OrderStockConfirmedIntegrationEvent".to_owned()),
    };

    let features = state
        .update_features(balance.clone(), movement, occurred_at)
        .await
        .ok_or_else(|| anyhow!("feature calculation failed for SKU {}", event.sku_id))?;

    let data_status = if balance_payload.authoritative {
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
    use chrono::Datelike;
    use data_platform_common::SkuLocation;
    use feature_engine::SkuLocationState;

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

    #[tokio::test]
    async fn trigger_filter_skips_when_stock_healthy_and_no_spike() {
        let state = AppState::new(OrchestratorConfig::from_env());
        let mut event = event_with_balance(true);
        // on_hand = 100, reorder_point = 20: 100 > 20 * 1.2 (= 24). Healthy stock, no spike.
        if let Some(balance) = &mut event.balance {
            balance.on_hand = 100;
            balance.reorder_point = 20;
            balance.safety_stock = 10;
        }

        // Seed established baseline so this normal depletion is not flagged as an anomalous spike
        let now = Utc::now();
        let mut sku_state = SkuLocationState::default();
        sku_state
            .model
            .observe_daily_total(now.weekday(), 2000.0, now);
        state
            .seed_feature_state(
                SkuLocation {
                    sku_id: event.sku_id,
                    location_code: "NCR".to_owned(),
                },
                sku_state,
            )
            .await;

        let outcome = handle_event(event, state.clone()).await.unwrap();
        assert_eq!(outcome, HandleOutcome::NoAction);
        let metrics = state.metrics().await;
        assert_eq!(metrics.llm_filter_skips, 1);
        assert_eq!(metrics.proposals_created, 0);
    }

    #[tokio::test]
    async fn trigger_filter_triggers_when_stock_at_reorder_threshold() {
        let state = AppState::new(OrchestratorConfig::from_env());
        let mut event = event_with_balance(true);
        // on_hand = 22, reorder_point = 20: 22 <= 20 * 1.2 (= 24). Within 20% of reorder point!
        if let Some(balance) = &mut event.balance {
            balance.on_hand = 22;
            balance.reorder_point = 20;
            balance.safety_stock = 10;
        }

        let outcome = handle_event(event, state.clone()).await.unwrap();
        assert_ne!(outcome, HandleOutcome::NoAction);
        let metrics = state.metrics().await;
        assert_eq!(metrics.llm_filter_skips, 0);
    }

    #[tokio::test]
    async fn queue_overflow_drops_event_and_increments_metric() {
        let mut config = OrchestratorConfig::from_env();
        config.llm.provider = crate::config::LlmProvider::Groq;
        config.llm.groq_tpm_budget = 1_000;
        config.llm.estimate_per_call = 1_000;
        config.llm.queue_max_depth = 1; // Only 1 waiter allowed in queue
        let state = AppState::new(config);

        // Pre-consume the budget so subsequent calls must queue
        state.rate_limiter.acquire().await.unwrap();

        // Spawn a task that fills the 1-slot wait queue
        let state2 = state.clone();
        let handle = tokio::spawn(async move {
            let _ = state2.rate_limiter.acquire().await;
        });

        // Give the task a moment to enter the wait queue
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        assert_eq!(state.rate_limiter.status().await.queue_depth, 1);

        // Now send an event that passes the trigger filter (near reorder point)
        let mut event = event_with_balance(true);
        if let Some(balance) = &mut event.balance {
            balance.on_hand = 15;
            balance.reorder_point = 20;
        }

        let outcome = handle_event(event, state.clone()).await.unwrap();
        assert_eq!(outcome, HandleOutcome::NoAction);

        let metrics = state.metrics().await;
        assert_eq!(metrics.llm_queue_overflow_drops, 1);
        assert_eq!(metrics.proposals_created, 0);

        handle.abort();
    }
}
