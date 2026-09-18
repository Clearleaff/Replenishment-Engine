use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use data_platform_common::{
    BALANCE_TOPIC, ChangeOperation, DebeziumDecoder, InventoryMovementType, MOVEMENT_TOPIC,
    NormalizedEvent, SkuLocation,
};
use feature_engine::{FeatureEngine, SkuLocationFeatures};
use futures::StreamExt;
use governance::{GovernanceStore, PostgresGovernanceStore};
use lakehouse::{
    KafkaEventIdentity, KafkaHeader, LocalBronzeLake, LocalSilverLake, PersistOutcome,
    RawKafkaEvent, RawKafkaEventInput, SilverInventoryEvent,
};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Headers, Timestamp},
};
use replenishment_agent::{
    AgentMode, ExecutionAttempt, GovernancePolicyConfig, LlmReasoner, MockLlmReasoner, PolicyGate,
    PolicyOutcome, ProposalExecutor, ProposalStatus, ReorderDecision, ReorderProposal,
    ReplenishmentAgent, ReplenishmentPolicyConfig, SimulationEngine,
};
use serde::Serialize;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;
use warehouse::{
    AnalyticalWarehouse, ClickHouseConfig, ClickHouseWarehouse, DeadLetterRecord, OpenReplenishment,
};

#[derive(Debug, Clone)]
struct Settings {
    kafka_bootstrap_servers: String,
    kafka_consumer_group: String,
    clickhouse: ClickHouseConfig,
    governance_connection_string: String,
    inventory_api_url: String,
    dashboard_bind: SocketAddr,
    agent_mode: AgentMode,
    simulated_lead_time_seconds: u64,
    data_lake_root: PathBuf,
}

impl Settings {
    fn from_environment() -> Result<Self> {
        let mode = match env_value("AGENT_MODE", "OBSERVE")
            .to_ascii_uppercase()
            .as_str()
        {
            "OBSERVE" => AgentMode::Observe,
            "RECOMMEND" => AgentMode::Recommend,
            "AUTO_DEMO" => AgentMode::AutoDemo,
            other => bail!("unsupported AGENT_MODE {other}"),
        };
        if mode == AgentMode::AutoDemo
            && !env_value("ESHOP_ENVIRONMENT", "Production").eq_ignore_ascii_case("Development")
        {
            bail!("AUTO_DEMO is allowed only when ESHOP_ENVIRONMENT=Development");
        }

        Ok(Self {
            kafka_bootstrap_servers: required("KAFKA_BOOTSTRAP_SERVERS")?,
            kafka_consumer_group: env_value("KAFKA_CONSUMER_GROUP", "eshop-rust-cdc-v1"),
            clickhouse: ClickHouseConfig {
                url: required("CLICKHOUSE_URL")?,
                database: env_value("CLICKHOUSE_DATABASE", "eshop_analytics"),
                user: env_value("CLICKHOUSE_USER", "eshop"),
                password: required("CLICKHOUSE_PASSWORD")?,
            },
            governance_connection_string: required("ConnectionStrings__governancedb")?,
            inventory_api_url: required("INVENTORY_API_URL")?,
            dashboard_bind: env_value("DASHBOARD_BIND", "127.0.0.1:8088")
                .parse()
                .context("DASHBOARD_BIND must be host:port")?,
            agent_mode: mode,
            simulated_lead_time_seconds: env_value("SIMULATED_LEAD_TIME_SECONDS", "15")
                .parse()
                .context("SIMULATED_LEAD_TIME_SECONDS must be an integer")?,
            data_lake_root: PathBuf::from(env_value("DATA_LAKE_ROOT", "data-lake")),
        })
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn env_value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[derive(Default)]
struct PipelineMetrics {
    consumed: AtomicU64,
    bronze_written: AtomicU64,
    bronze_duplicates: AtomicU64,
    silver_written: AtomicU64,
    silver_duplicates: AtomicU64,
    normalized: AtomicU64,
    duplicate_movements: AtomicU64,
    stale_balances: AtomicU64,
    dead_letters: AtomicU64,
    decisions: AtomicU64,
    simulations: AtomicU64,
    proposals: AtomicU64,
    policy_decisions: AtomicU64,
    executions: AtomicU64,
    execution_failures: AtomicU64,
    llm_failures: AtomicU64,
    restocks_completed: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardRecord {
    features: SkuLocationFeatures,
    last_decision: ReorderDecision,
    last_proposal: Option<ReorderProposal>,
    last_policy_outcome: Option<PolicyOutcome>,
    simulation_alternatives: usize,
    incoming_replenishment: i32,
}

#[derive(Clone)]
struct RuntimeState {
    settings: Settings,
    warehouse: Arc<ClickHouseWarehouse>,
    governance: Arc<PostgresGovernanceStore>,
    agent: Arc<ReplenishmentAgent>,
    bronze_lake: Arc<LocalBronzeLake>,
    silver_lake: Arc<LocalSilverLake>,
    engine: Arc<Mutex<FeatureEngine>>,
    dashboard: Arc<RwLock<HashMap<String, DashboardRecord>>>,
    open_replenishments: Arc<Mutex<HashMap<String, i32>>>,
    metrics: Arc<PipelineMetrics>,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    consumed: u64,
    bronze_written: u64,
    bronze_duplicates: u64,
    silver_written: u64,
    silver_duplicates: u64,
    normalized: u64,
    duplicate_movements: u64,
    stale_balances: u64,
    dead_letters: u64,
    decisions: u64,
    simulations: u64,
    proposals: u64,
    policy_decisions: u64,
    executions: u64,
    execution_failures: u64,
    llm_failures: u64,
    restocks_completed: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cdc_consumer=info,warn".into()),
        )
        .init();

    let settings = Settings::from_environment()?;
    let warehouse = Arc::new(ClickHouseWarehouse::new(settings.clickhouse.clone()));
    warehouse.initialize().await?;
    let governance =
        Arc::new(PostgresGovernanceStore::connect(&settings.governance_connection_string).await?);
    governance.initialize().await?;
    let recovery = warehouse.load_recovery_state().await?;

    let mut engine = FeatureEngine::default();
    for balance in recovery.balances {
        engine
            .state_mut(balance.sku_location())
            .apply_balance(balance);
    }
    for (key, model) in recovery.models {
        engine.state_mut(key).model = model;
    }
    let open_replenishments = recovery
        .open_replenishments
        .iter()
        .map(|item| {
            (
                SkuLocation::new(item.sku_id, &item.location_code).stream_key(),
                item.quantity,
            )
        })
        .collect();

    let state = RuntimeState {
        settings: settings.clone(),
        warehouse,
        governance,
        agent: Arc::new(ReplenishmentAgent::new(
            ReplenishmentPolicyConfig::default(),
            settings.agent_mode,
        )),
        bronze_lake: Arc::new(LocalBronzeLake::new(&settings.data_lake_root)),
        silver_lake: Arc::new(LocalSilverLake::new(&settings.data_lake_root)),
        engine: Arc::new(Mutex::new(engine)),
        dashboard: Arc::new(RwLock::new(HashMap::new())),
        open_replenishments: Arc::new(Mutex::new(open_replenishments)),
        metrics: Arc::new(PipelineMetrics::default()),
        http: reqwest::Client::new(),
    };

    for replenishment in recovery.open_replenishments {
        schedule_existing_replenishment(state.clone(), replenishment);
    }

    hydrate_recovery_dashboard(&state).await;

    let api_state = state.clone();
    let listener = tokio::net::TcpListener::bind(settings.dashboard_bind).await?;
    info!(address = %settings.dashboard_bind, "dashboard API listening");
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, dashboard_router(api_state)).await {
            error!(%error, "dashboard API stopped");
        }
    });

    consume(state).await
}

async fn hydrate_recovery_dashboard(state: &RuntimeState) {
    let projections = {
        let mut engine = state.engine.lock().await;
        engine
            .states_mut()
            .values_mut()
            .filter_map(|sku_state| {
                let balance = sku_state.balance.clone()?;
                sku_state
                    .calculate(Utc::now())
                    .map(|features| (features, balance))
            })
            .collect::<Vec<_>>()
    };

    let incoming = state.open_replenishments.lock().await.clone();
    let mut dashboard = state.dashboard.write().await;
    for (features, balance) in projections {
        let stream_key = features.key.stream_key();
        let incoming_replenishment = incoming.get(&stream_key).copied().unwrap_or(0);
        dashboard.insert(
            stream_key,
            DashboardRecord {
                last_decision: state
                    .agent
                    .evaluate(&features, &balance, incoming_replenishment),
                last_proposal: None,
                last_policy_outcome: None,
                simulation_alternatives: 0,
                features,
                incoming_replenishment,
            },
        );
    }
}

fn dashboard_router(state: RuntimeState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/sku-locations", get(list_sku_locations))
        .route(
            "/api/sku-locations/{sku_id}/{location_code}",
            get(get_sku_location),
        )
        .with_state(state)
}

async fn health(State(state): State<RuntimeState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "Healthy",
        consumed: state.metrics.consumed.load(Ordering::Relaxed),
        bronze_written: state.metrics.bronze_written.load(Ordering::Relaxed),
        bronze_duplicates: state.metrics.bronze_duplicates.load(Ordering::Relaxed),
        silver_written: state.metrics.silver_written.load(Ordering::Relaxed),
        silver_duplicates: state.metrics.silver_duplicates.load(Ordering::Relaxed),
        normalized: state.metrics.normalized.load(Ordering::Relaxed),
        duplicate_movements: state.metrics.duplicate_movements.load(Ordering::Relaxed),
        stale_balances: state.metrics.stale_balances.load(Ordering::Relaxed),
        dead_letters: state.metrics.dead_letters.load(Ordering::Relaxed),
        decisions: state.metrics.decisions.load(Ordering::Relaxed),
        simulations: state.metrics.simulations.load(Ordering::Relaxed),
        proposals: state.metrics.proposals.load(Ordering::Relaxed),
        policy_decisions: state.metrics.policy_decisions.load(Ordering::Relaxed),
        executions: state.metrics.executions.load(Ordering::Relaxed),
        execution_failures: state.metrics.execution_failures.load(Ordering::Relaxed),
        llm_failures: state.metrics.llm_failures.load(Ordering::Relaxed),
        restocks_completed: state.metrics.restocks_completed.load(Ordering::Relaxed),
    })
}

async fn list_sku_locations(State(state): State<RuntimeState>) -> Json<Vec<DashboardRecord>> {
    let mut records: Vec<_> = state.dashboard.read().await.values().cloned().collect();
    records.sort_by_key(|record| record.features.key.stream_key());
    Json(records)
}

async fn get_sku_location(
    State(state): State<RuntimeState>,
    Path((sku_id, location_code)): Path<(i32, String)>,
) -> Result<Json<DashboardRecord>, StatusCode> {
    state
        .dashboard
        .read()
        .await
        .get(&SkuLocation::new(sku_id, location_code).stream_key())
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn consume(state: RuntimeState) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &state.settings.kafka_consumer_group)
        .set("bootstrap.servers", &state.settings.kafka_bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .context("Kafka consumer creation failed")?;
    consumer.subscribe(&[MOVEMENT_TOPIC, BALANCE_TOPIC])?;
    info!(topics = ?[MOVEMENT_TOPIC, BALANCE_TOPIC], "CDC consumer subscribed");

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        let message = match result {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, "Kafka receive error");
                continue;
            }
        };
        state.metrics.consumed.fetch_add(1, Ordering::Relaxed);
        let identity = KafkaEventIdentity {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
        };
        let raw_event = raw_event_from_message(&message);
        match state.bronze_lake.persist(&raw_event)? {
            PersistOutcome::Written => {
                state.metrics.bronze_written.fetch_add(1, Ordering::Relaxed);
            }
            PersistOutcome::AlreadyExists => {
                state
                    .metrics
                    .bronze_duplicates
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        let payload = message.payload();
        match DebeziumDecoder::decode(message.topic(), payload) {
            Ok(event) => {
                let silver = SilverInventoryEvent::from_normalized(&identity, &event);
                match state.silver_lake.persist(&silver)? {
                    PersistOutcome::Written => {
                        state.metrics.silver_written.fetch_add(1, Ordering::Relaxed);
                    }
                    PersistOutcome::AlreadyExists => {
                        state
                            .metrics
                            .silver_duplicates
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                if let Err(error) = process_event(&state, event).await {
                    error!(
                        %error,
                        topic = message.topic(),
                        partition = message.partition(),
                        offset = message.offset(),
                        "normalized event processing failed; offset is not committed"
                    );
                    continue;
                }
                state.metrics.normalized.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                warn!(%error, "CDC record moved to dead-letter table");
                state
                    .warehouse
                    .insert_dead_letter(&DeadLetterRecord {
                        topic: message.topic().to_owned(),
                        partition: message.partition(),
                        offset: message.offset(),
                        error: error.to_string(),
                        payload: payload
                            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                            .unwrap_or_default(),
                        recorded_at: Utc::now(),
                    })
                    .await?;
                state.metrics.dead_letters.fetch_add(1, Ordering::Relaxed);
            }
        }
        consumer.commit_message(&message, CommitMode::Async)?;
    }
    Ok(())
}

fn raw_event_from_message<M>(message: &M) -> RawKafkaEvent
where
    M: Message,
{
    RawKafkaEvent::from_input(RawKafkaEventInput {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        event_timestamp: kafka_timestamp_to_utc(message.timestamp()),
        key: message.key(),
        value: message.payload(),
        headers: kafka_headers(message.headers()),
        ingestion_timestamp: Utc::now(),
    })
}

fn kafka_timestamp_to_utc(timestamp: Timestamp) -> Option<DateTime<Utc>> {
    timestamp
        .to_millis()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn kafka_headers<H>(headers: Option<&H>) -> Vec<KafkaHeader>
where
    H: Headers,
{
    headers
        .map(|headers| {
            headers
                .iter()
                .map(|header| KafkaHeader::new(header.key, header.value))
                .collect()
        })
        .unwrap_or_default()
}

async fn process_event(state: &RuntimeState, event: NormalizedEvent) -> Result<()> {
    match event {
        NormalizedEvent::Movement { fact, .. } => {
            if state.warehouse.movement_exists(fact.movement_id).await? {
                state
                    .metrics
                    .duplicate_movements
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            state.warehouse.insert_movement(&fact).await?;
            if !should_recalculate_on_movement(fact.movement_type) {
                return Ok(());
            }
            let key = fact.sku_location();
            let evaluation = {
                let mut engine = state.engine.lock().await;
                let sku_state = engine.state_mut(key.clone());
                sku_state.apply_movement(&fact);
                sku_state
                    .calculate(Utc::now())
                    .zip(sku_state.balance.clone())
            };
            if let Some((features, balance)) = evaluation {
                persist_evaluation(state, features, balance).await?;
            }
        }
        NormalizedEvent::Balance {
            operation,
            state: balance,
            ..
        } => {
            let key = balance.sku_location();
            let evaluation = {
                let mut engine = state.engine.lock().await;
                let sku_state = engine.state_mut(key);
                if operation == ChangeOperation::Snapshot {
                    sku_state.apply_balance_snapshot(balance.clone());
                    sku_state
                        .calculate(Utc::now())
                        .map(|features| (features, balance.clone()))
                } else if !sku_state.apply_balance(balance.clone()) {
                    None
                } else {
                    sku_state
                        .calculate(Utc::now())
                        .map(|features| (features, balance.clone()))
                }
            };
            if let Some((features, accepted_balance)) = evaluation {
                state.warehouse.upsert_balance(&accepted_balance).await?;
                persist_evaluation(state, features, accepted_balance).await?;
            } else {
                state.metrics.stale_balances.fetch_add(1, Ordering::Relaxed);
            }
        }
        NormalizedEvent::MovementDelete { movement_id, .. } => {
            warn!(%movement_id, "movement delete quarantined; historical facts are immutable");
        }
        NormalizedEvent::BalanceDelete { key, version, .. } => {
            warn!(stream_key = %key.stream_key(), version, "balance delete quarantined");
        }
        NormalizedEvent::Tombstone { topic } => {
            info!(%topic, "Kafka compaction tombstone acknowledged");
        }
    }
    Ok(())
}

fn should_recalculate_on_movement(movement_type: InventoryMovementType) -> bool {
    matches!(
        movement_type,
        InventoryMovementType::Reserve | InventoryMovementType::Sale
    )
}

async fn persist_evaluation(
    state: &RuntimeState,
    features: SkuLocationFeatures,
    balance: data_platform_common::InventoryBalanceState,
) -> Result<()> {
    let key = features.key.clone();
    let stream_key = key.stream_key();
    let incoming = state
        .open_replenishments
        .lock()
        .await
        .get(&stream_key)
        .copied()
        .unwrap_or(0);
    let decision = state.agent.evaluate(&features, &balance, incoming);
    let simulation = SimulationEngine::compare_reorder_quantities(
        &decision,
        &balance,
        incoming,
        &candidate_quantities(&decision, &balance, incoming),
    );
    let reasoning = match MockLlmReasoner.explain(&decision, &simulation) {
        Ok(reasoning) => reasoning,
        Err(error) => {
            state.metrics.llm_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("LLM reasoning failed safely: {error}"));
        }
    };
    let mut proposal =
        ReorderProposal::from_decision(&decision, simulation.selected_quantity, reasoning);
    let policy_gate = PolicyGate::new(policy_config_for_mode(state.settings.agent_mode));
    let policy = policy_gate.decide(
        &proposal,
        &decision,
        replenishment_agent::DataFreshnessStatus::Final,
    );
    if policy.outcome == PolicyOutcome::AutoApproved {
        proposal.status = ProposalStatus::Approved;
        proposal.updated_at = Utc::now();
    }
    let model = {
        let engine = state.engine.lock().await;
        engine
            .states()
            .get(&key)
            .map(|sku_state| sku_state.model.clone())
            .context("feature state disappeared during evaluation")?
    };

    state.warehouse.insert_features(&features).await?;
    state.warehouse.upsert_model(&key, &model).await?;
    state.warehouse.insert_decision(&decision).await?;
    state.governance.record_evaluation(&decision).await?;
    state.warehouse.insert_simulation(&simulation).await?;
    state.governance.record_simulation(&simulation).await?;
    state.warehouse.insert_proposal(&proposal).await?;
    state.governance.record_proposal(&proposal).await?;
    state.warehouse.insert_policy_decision(&policy).await?;
    state.governance.record_policy_decision(&policy).await?;
    state.metrics.decisions.fetch_add(1, Ordering::Relaxed);
    state.metrics.simulations.fetch_add(1, Ordering::Relaxed);
    state.metrics.proposals.fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .policy_decisions
        .fetch_add(1, Ordering::Relaxed);
    state.dashboard.write().await.insert(
        stream_key,
        DashboardRecord {
            features,
            last_decision: decision.clone(),
            last_proposal: Some(proposal.clone()),
            last_policy_outcome: Some(policy.outcome),
            simulation_alternatives: simulation.alternatives.len(),
            incoming_replenishment: incoming,
        },
    );
    maybe_schedule_replenishment(state.clone(), proposal, policy, balance).await?;
    Ok(())
}

fn candidate_quantities(
    decision: &ReorderDecision,
    balance: &data_platform_common::InventoryBalanceState,
    incoming: i32,
) -> Vec<i32> {
    let capacity = (balance.max_stock - balance.on_hand - incoming).max(0);
    let mut candidates = vec![20, decision.recommended_quantity, 81, 150, capacity];
    candidates.retain(|quantity| *quantity >= 0);
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn policy_config_for_mode(mode: AgentMode) -> GovernancePolicyConfig {
    GovernancePolicyConfig {
        allow_high_auto_execution: mode == AgentMode::AutoDemo,
        allow_critical_auto_execution: mode == AgentMode::AutoDemo,
        ..Default::default()
    }
}

async fn maybe_schedule_replenishment(
    state: RuntimeState,
    proposal: ReorderProposal,
    policy: replenishment_agent::PolicyDecision,
    balance: data_platform_common::InventoryBalanceState,
) -> Result<()> {
    if state.settings.agent_mode != AgentMode::AutoDemo || proposal.quantity <= 0 {
        return Ok(());
    }
    let attempt = match ProposalExecutor::validate_start(&proposal, &policy, &balance) {
        Ok(attempt) => attempt,
        Err(attempt) => {
            state.warehouse.insert_execution_attempt(&attempt).await?;
            state.governance.record_execution_attempt(&attempt).await?;
            state
                .metrics
                .execution_failures
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    };
    state.warehouse.insert_execution_attempt(&attempt).await?;
    state.governance.record_execution_attempt(&attempt).await?;
    state.metrics.executions.fetch_add(1, Ordering::Relaxed);

    let stream_key = proposal.key().stream_key();
    {
        let mut open = state.open_replenishments.lock().await;
        if open.contains_key(&stream_key) {
            return Ok(());
        }
        open.insert(stream_key, proposal.quantity);
    }
    let now = Utc::now();
    let replenishment = OpenReplenishment {
        operation_id: attempt.operation_id,
        sku_id: proposal.sku_id,
        location_code: proposal.location_code,
        quantity: proposal.quantity,
        status: "OPEN".to_owned(),
        created_at: now,
        updated_at: now,
    };
    state.warehouse.upsert_replenishment(&replenishment).await?;
    schedule_existing_replenishment(state, replenishment);
    Ok(())
}

fn schedule_existing_replenishment(state: RuntimeState, replenishment: OpenReplenishment) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(
            state.settings.simulated_lead_time_seconds,
        ))
        .await;
        if let Err(error) = complete_replenishment(&state, replenishment).await {
            error!(%error, "simulated replenishment failed");
        }
    });
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RestockRequest {
    operation_id: Uuid,
    sku_id: i32,
    location_code: String,
    quantity: i32,
    reason: String,
}

async fn complete_replenishment(
    state: &RuntimeState,
    mut replenishment: OpenReplenishment,
) -> Result<()> {
    let url = format!(
        "{}/api/inventory/restocks?api-version=1.0",
        state.settings.inventory_api_url.trim_end_matches('/')
    );
    let response = state
        .http
        .post(url)
        .json(&RestockRequest {
            operation_id: replenishment.operation_id,
            sku_id: replenishment.sku_id,
            location_code: replenishment.location_code.clone(),
            quantity: replenishment.quantity,
            reason: "AUTO_DEMO adaptive replenishment".to_owned(),
        })
        .send()
        .await
        .context("Inventory restock request failed")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::CONFLICT {
            replenishment.status = "NO_CAPACITY".to_owned();
            replenishment.updated_at = Utc::now();
            state.warehouse.upsert_replenishment(&replenishment).await?;
            state.open_replenishments.lock().await.remove(
                &SkuLocation::new(replenishment.sku_id, &replenishment.location_code).stream_key(),
            );
            let attempt = execution_result_attempt(
                replenishment.operation_id,
                "NO_CAPACITY",
                ProposalStatus::Failed,
                format!("Inventory returned conflict: {body}"),
            );
            state.warehouse.insert_execution_attempt(&attempt).await?;
            state.governance.record_execution_attempt(&attempt).await?;
            state
                .metrics
                .execution_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                operation_id = %replenishment.operation_id,
                sku_id = replenishment.sku_id,
                location = %replenishment.location_code,
                %body,
                "simulated replenishment closed because Inventory has no remaining capacity"
            );
            return Ok(());
        }
        bail!("Inventory restock returned {status}: {body}");
    }

    replenishment.status = "COMPLETED".to_owned();
    replenishment.updated_at = Utc::now();
    state.warehouse.upsert_replenishment(&replenishment).await?;
    let attempt = execution_result_attempt(
        replenishment.operation_id,
        "SUCCEEDED",
        ProposalStatus::Succeeded,
        "Inventory restock accepted".to_owned(),
    );
    state.warehouse.insert_execution_attempt(&attempt).await?;
    state.governance.record_execution_attempt(&attempt).await?;
    state
        .open_replenishments
        .lock()
        .await
        .remove(&SkuLocation::new(replenishment.sku_id, &replenishment.location_code).stream_key());
    state
        .metrics
        .restocks_completed
        .fetch_add(1, Ordering::Relaxed);
    info!(
        operation_id = %replenishment.operation_id,
        sku_id = replenishment.sku_id,
        location = %replenishment.location_code,
        quantity = replenishment.quantity,
        "simulated replenishment completed through Inventory API"
    );
    Ok(())
}

fn execution_result_attempt(
    operation_id: Uuid,
    result_code: &str,
    status: ProposalStatus,
    message: String,
) -> ExecutionAttempt {
    ExecutionAttempt {
        attempt_id: Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{operation_id}:{result_code}").as_bytes(),
        ),
        proposal_id: operation_id,
        operation_id,
        status,
        message,
        attempted_at: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parser_defaults_to_observe_logic() {
        assert_eq!(AgentMode::default(), AgentMode::Observe);
    }

    #[test]
    fn sku_location_dashboard_route_identity_is_normalized() {
        assert_eq!(SkuLocation::new(42, " ncr ").stream_key(), "42:NCR");
    }

    #[test]
    fn only_reserve_and_sale_movements_recalculate_before_balance_catches_up() {
        assert!(should_recalculate_on_movement(
            InventoryMovementType::Reserve
        ));
        assert!(should_recalculate_on_movement(InventoryMovementType::Sale));
        assert!(!should_recalculate_on_movement(
            InventoryMovementType::Restock
        ));
        assert!(!should_recalculate_on_movement(
            InventoryMovementType::Release
        ));
    }
}
