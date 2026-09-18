use chrono::{DateTime, Utc};
use data_platform_common::{InventoryBalanceState, InventoryMovementFact, SkuLocation};
use feature_engine::{SkuLocationFeatures, SkuLocationState};
use replenishment_agent::{
    DataFreshnessStatus, ExecutionAttempt, PolicyDecision, ProposalStatus, ReorderDecision,
    ReorderProposal, ReorderSimulation,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    config::OrchestratorConfig,
    llm_client::{LlmClient, LlmClientStatus},
};

#[derive(Debug, Clone, Serialize)]
pub struct AuditRecord {
    pub at: DateTime<Utc>,
    pub action: String,
    pub actor: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProposalRecord {
    pub proposal: ReorderProposal,
    pub decision: ReorderDecision,
    pub simulation: ReorderSimulation,
    pub policy: PolicyDecision,
    pub balance: InventoryBalanceState,
    pub features: SkuLocationFeatures,
    pub data_status: DataFreshnessStatus,
    pub source_event_key: String,
    pub status: ProposalStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub execution_attempts: Vec<ExecutionAttempt>,
    pub audit: Vec<AuditRecord>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ProposalRecordInput {
    pub proposal: ReorderProposal,
    pub decision: ReorderDecision,
    pub simulation: ReorderSimulation,
    pub policy: PolicyDecision,
    pub balance: InventoryBalanceState,
    pub features: SkuLocationFeatures,
    pub data_status: DataFreshnessStatus,
    pub source_event_key: String,
    pub message: String,
    pub ttl: std::time::Duration,
}

impl ProposalRecord {
    pub fn from_input(input: ProposalRecordInput) -> Self {
        let created_at = input.proposal.created_at;
        let expires_at = created_at
            + chrono::Duration::from_std(input.ttl).unwrap_or_else(|_| chrono::Duration::hours(1));
        let message = input.message;
        Self {
            status: input.proposal.status,
            proposal: input.proposal,
            decision: input.decision,
            simulation: input.simulation,
            policy: input.policy,
            balance: input.balance,
            features: input.features,
            data_status: input.data_status,
            source_event_key: input.source_event_key,
            created_at,
            updated_at: created_at,
            expires_at,
            execution_attempts: Vec::new(),
            audit: vec![AuditRecord {
                at: created_at,
                action: "PROPOSAL_RECORDED".to_owned(),
                actor: "hybrid-orchestrator".to_owned(),
                message: message.clone(),
            }],
            message,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OrchestratorMetrics {
    pub deliveries_seen: u64,
    pub events_processed: u64,
    pub invalid_events: u64,
    pub duplicate_events: u64,
    pub feature_skips: u64,
    pub proposals_created: u64,
    pub auto_approved: u64,
    pub human_approval_required: u64,
    pub rejected_by_policy: u64,
    pub approvals: u64,
    pub approval_misses: u64,
    pub llm_fallbacks: u64,
    pub llm_policy_rejections: u64,
    pub processing_errors: u64,
    pub amqp_reconnects: u64,
    pub expired_proposals: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub ready: bool,
    pub queue: String,
    pub execution_mode: crate::config::ExecutionMode,
    pub amqp_connected: bool,
    pub llm: LlmClientStatus,
    pub metrics: OrchestratorMetrics,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub config: Arc<OrchestratorConfig>,
    pub llm: LlmClient,
    proposals: Arc<Mutex<HashMap<Uuid, ProposalRecord>>>,
    seen_events: Arc<Mutex<HashSet<String>>>,
    feature_states: Arc<Mutex<HashMap<SkuLocation, SkuLocationState>>>,
    metrics: Arc<Mutex<OrchestratorMetrics>>,
    amqp_connected: Arc<Mutex<bool>>,
}

impl AppState {
    pub fn new(config: OrchestratorConfig) -> Self {
        let llm = LlmClient::new(config.llm.clone());
        Self {
            config: Arc::new(config),
            llm,
            proposals: Arc::new(Mutex::new(HashMap::new())),
            seen_events: Arc::new(Mutex::new(HashSet::new())),
            feature_states: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(OrchestratorMetrics::default())),
            amqp_connected: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn health(&self) -> HealthResponse {
        let amqp_connected = *self.amqp_connected.lock().await;
        let llm = self.llm.status().await;
        let ready = amqp_connected;
        HealthResponse {
            status: if ready { "Healthy" } else { "Degraded" }.to_owned(),
            ready,
            queue: self.config.queue_name.clone(),
            execution_mode: self.config.execution_mode,
            amqp_connected,
            llm,
            metrics: self.metrics().await,
        }
    }

    pub async fn set_amqp_connected(&self, connected: bool) {
        *self.amqp_connected.lock().await = connected;
    }

    pub async fn remember_event(&self, key: &str) -> bool {
        self.seen_events.lock().await.insert(key.to_owned())
    }

    pub async fn update_features(
        &self,
        balance: InventoryBalanceState,
        movement: InventoryMovementFact,
        as_of: DateTime<Utc>,
    ) -> Option<SkuLocationFeatures> {
        let key = balance.sku_location();
        let mut states = self.feature_states.lock().await;
        let state = states.entry(key).or_default();
        state.apply_balance_snapshot(balance);
        state.apply_movement(&movement);
        state.calculate(as_of)
    }

    pub async fn insert_proposal(&self, record: ProposalRecord) {
        self.proposals
            .lock()
            .await
            .insert(record.proposal.proposal_id, record);
    }

    pub async fn proposal(&self, proposal_id: Uuid) -> Option<ProposalRecord> {
        self.proposals.lock().await.get(&proposal_id).cloned()
    }

    pub async fn proposals(&self) -> Vec<ProposalRecord> {
        let mut records = self
            .proposals
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.created_at);
        records.reverse();
        records
    }

    pub async fn approve(&self, proposal_id: Uuid) -> Result<ProposalRecord, ApproveError> {
        let mut proposals = self.proposals.lock().await;
        let record = proposals
            .get_mut(&proposal_id)
            .ok_or(ApproveError::NotFound)?;

        if record.status != ProposalStatus::Proposed {
            return Err(ApproveError::NotPending(record.status));
        }
        if record.expires_at < Utc::now() {
            record.status = ProposalStatus::Failed;
            record.proposal.status = ProposalStatus::Failed;
            record.updated_at = Utc::now();
            record.audit.push(AuditRecord {
                at: record.updated_at,
                action: "APPROVAL_REJECTED_EXPIRED".to_owned(),
                actor: "boss-desk".to_owned(),
                message: "proposal expired before approval".to_owned(),
            });
            return Err(ApproveError::Expired);
        }

        record.status = ProposalStatus::Approved;
        record.proposal.status = ProposalStatus::Approved;
        record.proposal.updated_at = Utc::now();
        record.updated_at = record.proposal.updated_at;
        record.message = match self.config.execution_mode {
            crate::config::ExecutionMode::LogOnly => {
                "approved by human; LOG_ONLY command emission recorded without mutating Inventory"
                    .to_owned()
            }
        };
        record.audit.push(AuditRecord {
            at: record.updated_at,
            action: "APPROVED".to_owned(),
            actor: "boss-desk".to_owned(),
            message: record.message.clone(),
        });

        Ok(record.clone())
    }

    pub async fn cleanup_expired(&self) -> usize {
        let now = Utc::now();
        let mut proposals = self.proposals.lock().await;
        let before = proposals.len();
        proposals.retain(|_, record| {
            record.expires_at >= now || record.status != ProposalStatus::Proposed
        });
        let removed = before - proposals.len();
        if removed > 0 {
            self.incr(|metrics| metrics.expired_proposals += removed as u64)
                .await;
        }
        removed
    }

    pub async fn metrics(&self) -> OrchestratorMetrics {
        self.metrics.lock().await.clone()
    }

    pub async fn incr(&self, update: impl FnOnce(&mut OrchestratorMetrics)) {
        let mut metrics = self.metrics.lock().await;
        update(&mut metrics);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproveError {
    NotFound,
    NotPending(ProposalStatus),
    Expired,
}
