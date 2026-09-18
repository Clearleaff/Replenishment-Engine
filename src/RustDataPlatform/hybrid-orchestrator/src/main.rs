mod config;
mod event_handler;
mod llm_client;
mod state;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::json;
use tokio::{signal, time};
use tracing::{error, info};
use uuid::Uuid;

use config::OrchestratorConfig;
use state::{AppState, ApproveError};

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let health = state.health().await;
    let status = if health.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(health))
}

async fn list_proposals(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.proposals().await)
}

async fn get_proposal(
    Path(proposal_id): Path<Uuid>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.proposal(proposal_id).await {
        Some(record) => (StatusCode::OK, Json(json!(record))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "proposal not found", "proposalId": proposal_id })),
        )
            .into_response(),
    }
}

async fn approve_proposal(
    Path(proposal_id): Path<Uuid>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.approve(proposal_id).await {
        Ok(record) => {
            state.incr(|metrics| metrics.approvals += 1).await;
            info!(
                proposal_id = %record.proposal.proposal_id,
                sku_id = record.proposal.sku_id,
                location_code = %record.proposal.location_code,
                quantity = record.proposal.quantity,
                execution_mode = ?state.config.execution_mode,
                "human approved replenishment proposal"
            );

            (
                StatusCode::ACCEPTED,
                Json(json!({
                    "proposalId": record.proposal.proposal_id,
                    "status": record.status,
                    "executionMode": state.config.execution_mode,
                    "message": record.message,
                    "commandPreview": {
                        "operationId": record.proposal.proposal_id,
                        "skuId": record.proposal.sku_id,
                        "locationCode": record.proposal.location_code,
                        "quantity": record.proposal.quantity
                    }
                })),
            )
                .into_response()
        }
        Err(ApproveError::NotFound) => {
            state.incr(|metrics| metrics.approval_misses += 1).await;
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "proposal not found", "proposalId": proposal_id })),
            )
                .into_response()
        }
        Err(ApproveError::NotPending(status)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "proposal is not pending approval",
                "proposalId": proposal_id,
                "currentStatus": status
            })),
        )
            .into_response(),
        Err(ApproveError::Expired) => (
            StatusCode::GONE,
            Json(json!({
                "error": "proposal expired before approval",
                "proposalId": proposal_id
            })),
        )
            .into_response(),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "hybrid_orchestrator=info,info".to_owned()),
        )
        .init();

    let state = AppState::new(OrchestratorConfig::from_env());
    let listener = tokio::net::TcpListener::bind(state.config.bind_addr).await?;

    let llm_status = state.llm.status().await;
    info!(
        provider = ?llm_status.provider,
        configured = llm_status.configured,
        model = %llm_status.model,
        "LLM strategy officer initialized"
    );

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/proposals", get(list_proposals))
        .route("/api/v1/proposals/{proposal_id}", get(get_proposal))
        .route(
            "/api/v1/proposals/{proposal_id}/approve",
            post(approve_proposal),
        )
        .with_state(state.clone());

    info!(
        bind_addr = %state.config.bind_addr,
        queue = %state.config.queue_name,
        amqp_url_configured = !state.config.amqp_url.is_empty(),
        execution_mode = ?state.config.execution_mode,
        "hybrid orchestrator starting"
    );

    let http_task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            error!(?error, "HTTP server stopped with error");
        }
    });

    let consumer_state = state.clone();
    let amqp_task = tokio::spawn(async move {
        if let Err(error) = event_handler::start_consumer(consumer_state).await {
            error!(?error, "AMQP consumer stopped with unrecoverable error");
        }
    });

    let cleanup_state = state.clone();
    let cleanup_task = tokio::spawn(async move {
        let mut interval = time::interval(cleanup_state.config.cleanup_interval);
        loop {
            interval.tick().await;
            let removed = cleanup_state.cleanup_expired().await;
            if removed > 0 {
                info!(removed, "expired pending proposals cleaned up");
            }
        }
    });

    tokio::select! {
        _ = shutdown_signal() => {
            info!("shutdown signal received; stopping hybrid orchestrator");
        }
        _ = http_task => {}
        _ = amqp_task => {}
        _ = cleanup_task => {}
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("installing Ctrl-C handler should succeed");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler should succeed")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
