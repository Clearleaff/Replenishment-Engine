use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
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
use chrono::Utc;
use data_platform_common::{
    BALANCE_TOPIC, DebeziumDecoder, InventoryMovementType, MOVEMENT_TOPIC, NormalizedEvent,
    SkuLocation,
};
use feature_engine::{FeatureEngine, SkuLocationFeatures};
use futures::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use replenishment_agent::{
    AgentMode, ReorderDecision, ReplenishmentAgent, ReplenishmentPolicyConfig,
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
    inventory_api_url: String,
    dashboard_bind: SocketAddr,
    agent_mode: AgentMode,
    simulated_lead_time_seconds: u64,
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
            inventory_api_url: required("INVENTORY_API_URL")?,
            dashboard_bind: env_value("DASHBOARD_BIND", "127.0.0.1:8088")
                .parse()
                .context("DASHBOARD_BIND must be host:port")?,
            agent_mode: mode,
            simulated_lead_time_seconds: env_value("SIMULATED_LEAD_TIME_SECONDS", "15")
                .parse()
                .context("SIMULATED_LEAD_TIME_SECONDS must be an integer")?,
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
    normalized: AtomicU64,
    duplicate_movements: AtomicU64,
    stale_balances: AtomicU64,
    dead_letters: AtomicU64,
    decisions: AtomicU64,
    restocks_completed: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardRecord {
    features: SkuLocationFeatures,
    last_decision: ReorderDecision,
    incoming_replenishment: i32,
}

#[derive(Clone)]
struct RuntimeState {
    settings: Settings,
    warehouse: Arc<ClickHouseWarehouse>,
    agent: Arc<ReplenishmentAgent>,
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
    normalized: u64,
    duplicate_movements: u64,
    stale_balances: u64,
    dead_letters: u64,
    decisions: u64,
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
        agent: Arc::new(ReplenishmentAgent::new(
            ReplenishmentPolicyConfig::default(),
            settings.agent_mode,
        )),
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
        normalized: state.metrics.normalized.load(Ordering::Relaxed),
        duplicate_movements: state.metrics.duplicate_movements.load(Ordering::Relaxed),
        stale_balances: state.metrics.stale_balances.load(Ordering::Relaxed),
        dead_letters: state.metrics.dead_letters.load(Ordering::Relaxed),
        decisions: state.metrics.decisions.load(Ordering::Relaxed),
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
        let payload = message.payload();
        match DebeziumDecoder::decode(message.topic(), payload) {
            Ok(event) => {
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
        NormalizedEvent::Balance { state: balance, .. } => {
            let key = balance.sku_location();
            let evaluation = {
                let mut engine = state.engine.lock().await;
                let sku_state = engine.state_mut(key);
                if !sku_state.apply_balance(balance.clone()) {
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
    state.metrics.decisions.fetch_add(1, Ordering::Relaxed);
    state.dashboard.write().await.insert(
        stream_key,
        DashboardRecord {
            features,
            last_decision: decision.clone(),
            incoming_replenishment: incoming,
        },
    );
    maybe_schedule_replenishment(state.clone(), decision).await?;
    Ok(())
}

async fn maybe_schedule_replenishment(
    state: RuntimeState,
    decision: ReorderDecision,
) -> Result<()> {
    if state.settings.agent_mode != AgentMode::AutoDemo || decision.recommended_quantity <= 0 {
        return Ok(());
    }
    let stream_key = SkuLocation::new(decision.sku_id, &decision.location_code).stream_key();
    {
        let mut open = state.open_replenishments.lock().await;
        if open.contains_key(&stream_key) {
            return Ok(());
        }
        open.insert(stream_key, decision.recommended_quantity);
    }
    let now = Utc::now();
    let replenishment = OpenReplenishment {
        operation_id: decision.decision_id,
        sku_id: decision.sku_id,
        location_code: decision.location_code,
        quantity: decision.recommended_quantity,
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
