use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use data_platform_common::{InventoryBalanceState, InventoryMovementFact, SkuLocation};
use feature_engine::{OnlineDemandModel, SkuLocationFeatures};
use replenishment_agent::{
    ExecutionAttempt, PolicyDecision, ReorderDecision, ReorderProposal, ReorderSimulation,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[async_trait]
pub trait AnalyticalWarehouse: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn movement_exists(&self, movement_id: Uuid) -> Result<bool>;
    async fn insert_movement(&self, fact: &InventoryMovementFact) -> Result<()>;
    async fn upsert_balance(&self, state: &InventoryBalanceState) -> Result<()>;
    async fn insert_features(&self, features: &SkuLocationFeatures) -> Result<()>;
    async fn upsert_model(&self, key: &SkuLocation, model: &OnlineDemandModel) -> Result<()>;
    async fn insert_decision(&self, decision: &ReorderDecision) -> Result<()>;
    async fn insert_simulation(&self, simulation: &ReorderSimulation) -> Result<()>;
    async fn insert_proposal(&self, proposal: &ReorderProposal) -> Result<()>;
    async fn insert_policy_decision(&self, decision: &PolicyDecision) -> Result<()>;
    async fn insert_execution_attempt(&self, attempt: &ExecutionAttempt) -> Result<()>;
    async fn insert_dead_letter(&self, record: &DeadLetterRecord) -> Result<()>;
    async fn upsert_replenishment(&self, replenishment: &OpenReplenishment) -> Result<()>;
    async fn load_recovery_state(&self) -> Result<RecoveryState>;
    async fn query_event_calendar(
        &self,
        location_code: &str,
        days_ahead: u32,
    ) -> Result<Vec<EventCalendarEntry>>;
    async fn query_supplier_signals(
        &self,
        sku_id: Option<i32>,
        location_code: &str,
    ) -> Result<Vec<SupplierSignalEntry>>;
    async fn query_historical_daily_sales(
        &self,
        sku_id: i32,
        location_code: &str,
        days_back: u32,
    ) -> Result<Vec<DailySalesEntry>>;
    async fn insert_event_calendar_entry(&self, entry: &EventCalendarEntry) -> Result<()>;
    async fn insert_supplier_signal(&self, signal: &SupplierSignalEntry) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct ClickHouseWarehouse {
    client: Client,
    config: ClickHouseConfig,
}

impl ClickHouseWarehouse {
    pub fn new(config: ClickHouseConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    async fn execute(&self, sql: &str) -> Result<String> {
        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .query(&[("date_time_output_format", "iso")])
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(sql.to_owned())
            .send()
            .await
            .context("ClickHouse request failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("ClickHouse response failed")?;
        if !status.is_success() {
            bail!("ClickHouse returned {status}: {body}");
        }
        Ok(body)
    }

    async fn insert_json<T: Serialize + Sync>(&self, table: &str, row: &T) -> Result<()> {
        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            self.config.database, table
        );
        let mut body = serde_json::to_vec(row).context("warehouse row serialization failed")?;
        body.push(b'\n');
        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .query(&[("query", query)])
            .body(body)
            .send()
            .await
            .context("ClickHouse insert failed")?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("ClickHouse insert into {table} returned {status}: {body}");
        }
        Ok(())
    }
}

#[async_trait]
impl AnalyticalWarehouse for ClickHouseWarehouse {
    async fn initialize(&self) -> Result<()> {
        self.execute(&format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            self.config.database
        ))
        .await?;

        for statement in schema_statements(&self.config.database) {
            self.execute(&statement).await?;
        }
        Ok(())
    }

    async fn movement_exists(&self, movement_id: Uuid) -> Result<bool> {
        let sql = format!(
            "SELECT count() FROM {}.fact_inventory_movements FINAL WHERE movement_id = toUUID('{}') FORMAT TabSeparated",
            self.config.database, movement_id
        );
        Ok(self.execute(&sql).await?.trim() != "0")
    }

    async fn insert_movement(&self, fact: &InventoryMovementFact) -> Result<()> {
        self.insert_json("fact_inventory_movements", &MovementRow::from(fact))
            .await
    }

    async fn upsert_balance(&self, state: &InventoryBalanceState) -> Result<()> {
        self.insert_json("inventory_balance_current", &BalanceRow::from(state))
            .await
    }

    async fn insert_features(&self, features: &SkuLocationFeatures) -> Result<()> {
        self.insert_json("sku_location_minute_features", &FeatureRow::from(features))
            .await
    }

    async fn upsert_model(&self, key: &SkuLocation, model: &OnlineDemandModel) -> Result<()> {
        self.insert_json(
            "sku_location_model_state",
            &ModelRow {
                sku_id: key.sku_id,
                location_code: key.location_code.clone(),
                model_json: serde_json::to_string(model)?,
                updated_at: Utc::now(),
            },
        )
        .await
    }

    async fn insert_decision(&self, decision: &ReorderDecision) -> Result<()> {
        self.insert_json("reorder_decisions", &DecisionRow::from(decision))
            .await
    }

    async fn insert_simulation(&self, simulation: &ReorderSimulation) -> Result<()> {
        self.insert_json("reorder_simulations", &SimulationRow::from(simulation))
            .await
    }

    async fn insert_proposal(&self, proposal: &ReorderProposal) -> Result<()> {
        self.insert_json("reorder_proposals", &ProposalRow::from(proposal))
            .await
    }

    async fn insert_policy_decision(&self, decision: &PolicyDecision) -> Result<()> {
        self.insert_json("policy_decisions", &PolicyDecisionRow::from(decision))
            .await
    }

    async fn insert_execution_attempt(&self, attempt: &ExecutionAttempt) -> Result<()> {
        self.insert_json("execution_attempts", &ExecutionAttemptRow::from(attempt))
            .await
    }

    async fn insert_dead_letter(&self, record: &DeadLetterRecord) -> Result<()> {
        self.insert_json("cdc_dead_letters", record).await
    }

    async fn upsert_replenishment(&self, replenishment: &OpenReplenishment) -> Result<()> {
        self.insert_json("open_replenishments", replenishment).await
    }

    async fn load_recovery_state(&self) -> Result<RecoveryState> {
        let balance_sql = format!(
            "SELECT sku_id,location_code,on_hand,reserved,available,safety_stock,reorder_point,max_stock,version,updated_at FROM {}.inventory_balance_current FINAL FORMAT JSONEachRow",
            self.config.database
        );
        let model_sql = format!(
            "SELECT sku_id,location_code,model_json,updated_at FROM {}.sku_location_model_state FINAL FORMAT JSONEachRow",
            self.config.database
        );
        let replenishment_sql = format!(
            "SELECT operation_id,sku_id,location_code,quantity,status,created_at,updated_at FROM {}.open_replenishments FINAL WHERE status = 'OPEN' FORMAT JSONEachRow",
            self.config.database
        );

        let balances =
            parse_json_lines::<InventoryBalanceState>(&self.execute(&balance_sql).await?)?;
        let model_rows = parse_json_lines::<ModelRow>(&self.execute(&model_sql).await?)?;
        let models = model_rows
            .into_iter()
            .map(|row| {
                let model = serde_json::from_str(&row.model_json)
                    .context("persisted online model JSON is invalid")?;
                Ok((SkuLocation::new(row.sku_id, row.location_code), model))
            })
            .collect::<Result<Vec<_>>>()?;
        let open_replenishments =
            parse_json_lines::<OpenReplenishment>(&self.execute(&replenishment_sql).await?)?;
        Ok(RecoveryState {
            balances,
            models,
            open_replenishments,
        })
    }

    async fn query_event_calendar(
        &self,
        location_code: &str,
        days_ahead: u32,
    ) -> Result<Vec<EventCalendarEntry>> {
        let loc = location_code.trim().to_ascii_uppercase();
        let sql = format!(
            "SELECT event_id,location_code,event_name,event_type,start_date,end_date,expected_demand_impact,notes,created_at,updated_at \
             FROM {}.event_calendar FINAL \
             WHERE (location_code = '{loc}' OR location_code = 'ALL') \
               AND start_date <= today() + {days_ahead} \
               AND end_date >= today() \
             ORDER BY start_date ASC FORMAT JSONEachRow",
            self.config.database
        );
        parse_json_lines::<EventCalendarEntry>(&self.execute(&sql).await?)
    }

    async fn query_supplier_signals(
        &self,
        sku_id: Option<i32>,
        location_code: &str,
    ) -> Result<Vec<SupplierSignalEntry>> {
        let loc = location_code.trim().to_ascii_uppercase();
        let sku_filter = match sku_id {
            Some(id) => format!("AND (sku_id = {id} OR sku_id IS NULL)"),
            None => String::new(),
        };
        let sql = format!(
            "SELECT signal_id,sku_id,location_code,signal_type,severity,description,reported_at,resolved_at,source,created_at \
             FROM {}.supplier_disruption_signals FINAL \
             WHERE (location_code = '{loc}' OR location_code = 'ALL') \
               {sku_filter} \
               AND (resolved_at IS NULL OR resolved_at >= now() - INTERVAL 7 DAY) \
             ORDER BY reported_at DESC FORMAT JSONEachRow",
            self.config.database
        );
        parse_json_lines::<SupplierSignalEntry>(&self.execute(&sql).await?)
    }

    async fn query_historical_daily_sales(
        &self,
        sku_id: i32,
        location_code: &str,
        days_back: u32,
    ) -> Result<Vec<DailySalesEntry>> {
        let loc = location_code.trim().to_ascii_uppercase();
        let sql = format!(
            "SELECT toDate(occurred_at) AS date, toInt64(sum(quantity)) AS total_units \
             FROM {}.fact_inventory_movements \
             WHERE sku_id = {sku_id} \
               AND location_code = '{loc}' \
               AND movement_type = 'Sale' \
               AND occurred_at >= now() - INTERVAL {days_back} DAY \
             GROUP BY date \
             ORDER BY date ASC FORMAT JSONEachRow",
            self.config.database
        );
        parse_json_lines::<DailySalesEntry>(&self.execute(&sql).await?)
    }

    async fn insert_event_calendar_entry(&self, entry: &EventCalendarEntry) -> Result<()> {
        self.insert_json("event_calendar", entry).await
    }

    async fn insert_supplier_signal(&self, signal: &SupplierSignalEntry) -> Result<()> {
        self.insert_json("supplier_disruption_signals", signal)
            .await
    }
}

fn parse_json_lines<T: for<'de> Deserialize<'de>>(body: &str) -> Result<Vec<T>> {
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).context("invalid ClickHouse JSONEachRow response"))
        .collect()
}

pub fn schema_statements(database: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.fact_inventory_movements (movement_id UUID, source_event_id UUID, sku_id Int32, location_code LowCardinality(String), order_id Nullable(Int32), movement_type LowCardinality(String), quantity Int32, occurred_at DateTime64(6, 'UTC'), recorded_at DateTime64(6, 'UTC'), resulting_balance_version Int64, reason Nullable(String)) ENGINE = ReplacingMergeTree(recorded_at) PARTITION BY toYYYYMM(occurred_at) ORDER BY movement_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.inventory_balance_current (sku_id Int32, location_code LowCardinality(String), on_hand Int32, reserved Int32, available Int32, safety_stock Int32, reorder_point Int32, max_stock Int32, version Int64, updated_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(version) ORDER BY (sku_id, location_code)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.sku_location_minute_features (sku_id Int32, location_code LowCardinality(String), computed_at DateTime64(6, 'UTC'), on_hand Int32, reserved Int32, available Int32, balance_version Int64, reserve_units_5m Int32, sale_units_5m Int32, sale_units_15m Int32, sale_units_1h Int32, sale_velocity_5m Float64, sale_velocity_15m Float64, sale_velocity_1h Float64, daily_sales Int32, baseline_units_per_day Float64, recent_units_per_day Float64, forecast_units_per_day Float64, historical_stddev_units_per_day Float64, recent_vs_baseline_ratio Float64, spike_score Float64, spike_detected Bool) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(computed_at) ORDER BY (sku_id, location_code, toStartOfMinute(computed_at))"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.sku_location_model_state (sku_id Int32, location_code LowCardinality(String), model_json String, updated_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (sku_id, location_code)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.reorder_decisions (decision_id UUID, sku_id Int32, location_code LowCardinality(String), created_at DateTime64(6, 'UTC'), forecast_units_per_day Float64, baseline_units_per_day Float64, recent_units_per_day Float64, spike_score Float64, risk_level LowCardinality(String), estimated_stockout_at Nullable(DateTime64(6, 'UTC')), inventory_position Int32, safety_stock Int32, target_inventory Int32, recommended_quantity Int32, horizon_demand_units Float64, horizon_forecast_json String, reason_codes Array(String), model_version String, mode LowCardinality(String)) ENGINE = ReplacingMergeTree(created_at) PARTITION BY toYYYYMM(created_at) ORDER BY decision_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.reorder_simulations (simulation_id UUID, decision_id UUID, sku_id Int32, location_code LowCardinality(String), evaluated_at DateTime64(6, 'UTC'), selected_quantity Int32, alternatives_json String) ENGINE = ReplacingMergeTree(evaluated_at) PARTITION BY toYYYYMM(evaluated_at) ORDER BY simulation_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.reorder_proposals (proposal_id UUID, decision_id UUID, sku_id Int32, location_code LowCardinality(String), quantity Int32, status LowCardinality(String), created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC'), correlation_id UUID, reasoning_json String) ENGINE = ReplacingMergeTree(updated_at) PARTITION BY toYYYYMM(created_at) ORDER BY proposal_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.policy_decisions (policy_decision_id UUID, proposal_id UUID, outcome LowCardinality(String), reason_codes Array(String), policy_version String, decided_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(decided_at) PARTITION BY toYYYYMM(decided_at) ORDER BY policy_decision_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.execution_attempts (attempt_id UUID, proposal_id UUID, operation_id UUID, status LowCardinality(String), message String, attempted_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(attempted_at) PARTITION BY toYYYYMM(attempted_at) ORDER BY attempt_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.open_replenishments (operation_id UUID, sku_id Int32, location_code LowCardinality(String), quantity Int32, status LowCardinality(String), created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(updated_at) ORDER BY operation_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.cdc_dead_letters (topic String, partition Int32, offset Int64, error String, payload String, recorded_at DateTime64(6, 'UTC')) ENGINE = MergeTree PARTITION BY toYYYYMM(recorded_at) ORDER BY (topic, partition, offset)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.event_calendar (event_id UUID, location_code LowCardinality(String), event_name String, event_type LowCardinality(String), start_date Date, end_date Date, expected_demand_impact LowCardinality(String), notes Nullable(String), created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (location_code, start_date, event_id)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.supplier_disruption_signals (signal_id UUID, sku_id Nullable(Int32), location_code LowCardinality(String), signal_type LowCardinality(String), severity LowCardinality(String), description String, reported_at DateTime64(6, 'UTC'), resolved_at Nullable(DateTime64(6, 'UTC')), source LowCardinality(String), created_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(created_at) ORDER BY (location_code, reported_at, signal_id)"
        ),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventCalendarEntry {
    pub event_id: Uuid,
    pub location_code: String,
    pub event_name: String,
    pub event_type: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub expected_demand_impact: String,
    pub notes: Option<String>,
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "chrono::Utc::now")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SupplierSignalEntry {
    pub signal_id: Uuid,
    pub sku_id: Option<i32>,
    pub location_code: String,
    pub signal_type: String,
    pub severity: String,
    pub description: String,
    pub reported_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub source: String,
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailySalesEntry {
    #[serde(alias = "sale_date")]
    pub date: String,
    pub total_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: String,
    pub payload: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenReplenishment {
    pub operation_id: Uuid,
    pub sku_id: i32,
    pub location_code: String,
    pub quantity: i32,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct RecoveryState {
    pub balances: Vec<InventoryBalanceState>,
    pub models: Vec<(SkuLocation, OnlineDemandModel)>,
    pub open_replenishments: Vec<OpenReplenishment>,
}

#[derive(Serialize)]
struct MovementRow {
    movement_id: Uuid,
    source_event_id: Uuid,
    sku_id: i32,
    location_code: String,
    order_id: Option<i32>,
    movement_type: String,
    quantity: i32,
    occurred_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    resulting_balance_version: i64,
    reason: Option<String>,
}

impl From<&InventoryMovementFact> for MovementRow {
    fn from(value: &InventoryMovementFact) -> Self {
        Self {
            movement_id: value.movement_id,
            source_event_id: value.source_event_id,
            sku_id: value.sku_id,
            location_code: value.location_code.clone(),
            order_id: value.order_id,
            movement_type: format!("{:?}", value.movement_type),
            quantity: value.quantity,
            occurred_at: value.occurred_at,
            recorded_at: value.recorded_at,
            resulting_balance_version: value.resulting_balance_version,
            reason: value.reason.clone(),
        }
    }
}

#[derive(Serialize)]
struct BalanceRow {
    sku_id: i32,
    location_code: String,
    on_hand: i32,
    reserved: i32,
    available: i32,
    safety_stock: i32,
    reorder_point: i32,
    max_stock: i32,
    version: i64,
    updated_at: DateTime<Utc>,
}

impl From<&InventoryBalanceState> for BalanceRow {
    fn from(value: &InventoryBalanceState) -> Self {
        Self {
            sku_id: value.sku_id,
            location_code: value.location_code.clone(),
            on_hand: value.on_hand,
            reserved: value.reserved,
            available: value.available,
            safety_stock: value.safety_stock,
            reorder_point: value.reorder_point,
            max_stock: value.max_stock,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Serialize)]
struct FeatureRow {
    sku_id: i32,
    location_code: String,
    computed_at: DateTime<Utc>,
    on_hand: i32,
    reserved: i32,
    available: i32,
    balance_version: i64,
    reserve_units_5m: i32,
    sale_units_5m: i32,
    sale_units_15m: i32,
    sale_units_1h: i32,
    sale_velocity_5m: f64,
    sale_velocity_15m: f64,
    sale_velocity_1h: f64,
    daily_sales: i32,
    baseline_units_per_day: f64,
    recent_units_per_day: f64,
    forecast_units_per_day: f64,
    historical_stddev_units_per_day: f64,
    recent_vs_baseline_ratio: f64,
    spike_score: f64,
    spike_detected: bool,
}

impl From<&SkuLocationFeatures> for FeatureRow {
    fn from(value: &SkuLocationFeatures) -> Self {
        Self {
            sku_id: value.key.sku_id,
            location_code: value.key.location_code.clone(),
            computed_at: value.computed_at,
            on_hand: value.on_hand,
            reserved: value.reserved,
            available: value.available,
            balance_version: value.balance_version,
            reserve_units_5m: value.reserve_units_5m,
            sale_units_5m: value.sale_units_5m,
            sale_units_15m: value.sale_units_15m,
            sale_units_1h: value.sale_units_1h,
            sale_velocity_5m: value.sale_velocity_5m,
            sale_velocity_15m: value.sale_velocity_15m,
            sale_velocity_1h: value.sale_velocity_1h,
            daily_sales: value.daily_sales,
            baseline_units_per_day: value.forecast.baseline_units_per_day,
            recent_units_per_day: value.forecast.recent_units_per_day,
            forecast_units_per_day: value.forecast.forecast_units_per_day,
            historical_stddev_units_per_day: value.forecast.historical_stddev_units_per_day,
            recent_vs_baseline_ratio: value.forecast.recent_vs_baseline_ratio,
            spike_score: value.forecast.spike_score,
            spike_detected: value.forecast.spike_detected,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelRow {
    sku_id: i32,
    location_code: String,
    model_json: String,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct DecisionRow {
    decision_id: Uuid,
    sku_id: i32,
    location_code: String,
    created_at: DateTime<Utc>,
    forecast_units_per_day: f64,
    baseline_units_per_day: f64,
    recent_units_per_day: f64,
    spike_score: f64,
    risk_level: String,
    estimated_stockout_at: Option<DateTime<Utc>>,
    inventory_position: i32,
    safety_stock: i32,
    target_inventory: i32,
    recommended_quantity: i32,
    horizon_demand_units: f64,
    horizon_forecast_json: String,
    reason_codes: Vec<String>,
    model_version: String,
    mode: String,
}

impl From<&ReorderDecision> for DecisionRow {
    fn from(value: &ReorderDecision) -> Self {
        Self {
            decision_id: value.decision_id,
            sku_id: value.sku_id,
            location_code: value.location_code.clone(),
            created_at: value.created_at,
            forecast_units_per_day: value.forecast_units_per_day,
            baseline_units_per_day: value.baseline_units_per_day,
            recent_units_per_day: value.recent_units_per_day,
            spike_score: value.spike_score,
            risk_level: format!("{:?}", value.risk_level).to_ascii_uppercase(),
            estimated_stockout_at: value.estimated_stockout_at,
            inventory_position: value.inventory_position,
            safety_stock: value.safety_stock,
            target_inventory: value.target_inventory,
            recommended_quantity: value.recommended_quantity,
            horizon_demand_units: value.horizon_demand_units,
            horizon_forecast_json: serde_json::to_string(&value.horizon_forecast)
                .unwrap_or_else(|_| "[]".to_owned()),
            reason_codes: value.reason_codes.clone(),
            model_version: value.model_version.clone(),
            mode: format!("{:?}", value.mode).to_ascii_uppercase(),
        }
    }
}

#[derive(Serialize)]
struct SimulationRow {
    simulation_id: Uuid,
    decision_id: Uuid,
    sku_id: i32,
    location_code: String,
    evaluated_at: DateTime<Utc>,
    selected_quantity: i32,
    alternatives_json: String,
}

impl From<&ReorderSimulation> for SimulationRow {
    fn from(value: &ReorderSimulation) -> Self {
        Self {
            simulation_id: value.simulation_id,
            decision_id: value.decision_id,
            sku_id: value.sku_id,
            location_code: value.location_code.clone(),
            evaluated_at: value.evaluated_at,
            selected_quantity: value.selected_quantity,
            alternatives_json: serde_json::to_string(&value.alternatives)
                .unwrap_or_else(|_| "[]".to_owned()),
        }
    }
}

#[derive(Serialize)]
struct ProposalRow {
    proposal_id: Uuid,
    decision_id: Uuid,
    sku_id: i32,
    location_code: String,
    quantity: i32,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    correlation_id: Uuid,
    reasoning_json: String,
}

impl From<&ReorderProposal> for ProposalRow {
    fn from(value: &ReorderProposal) -> Self {
        Self {
            proposal_id: value.proposal_id,
            decision_id: value.decision_id,
            sku_id: value.sku_id,
            location_code: value.location_code.clone(),
            quantity: value.quantity,
            status: format!("{:?}", value.status).to_ascii_uppercase(),
            created_at: value.created_at,
            updated_at: value.updated_at,
            correlation_id: value.correlation_id,
            reasoning_json: serde_json::to_string(&value.reasoning)
                .unwrap_or_else(|_| "{}".to_owned()),
        }
    }
}

#[derive(Serialize)]
struct PolicyDecisionRow {
    policy_decision_id: Uuid,
    proposal_id: Uuid,
    outcome: String,
    reason_codes: Vec<String>,
    policy_version: String,
    decided_at: DateTime<Utc>,
}

impl From<&PolicyDecision> for PolicyDecisionRow {
    fn from(value: &PolicyDecision) -> Self {
        Self {
            policy_decision_id: value.policy_decision_id,
            proposal_id: value.proposal_id,
            outcome: format!("{:?}", value.outcome).to_ascii_uppercase(),
            reason_codes: value.reason_codes.clone(),
            policy_version: value.policy_version.clone(),
            decided_at: value.decided_at,
        }
    }
}

#[derive(Serialize)]
struct ExecutionAttemptRow {
    attempt_id: Uuid,
    proposal_id: Uuid,
    operation_id: Uuid,
    status: String,
    message: String,
    attempted_at: DateTime<Utc>,
}

impl From<&ExecutionAttempt> for ExecutionAttemptRow {
    fn from(value: &ExecutionAttempt) -> Self {
        Self {
            attempt_id: value.attempt_id,
            proposal_id: value.proposal_id,
            operation_id: value.operation_id,
            status: format!("{:?}", value.status).to_ascii_uppercase(),
            message: value.message.clone(),
            attempted_at: value.attempted_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_contains_all_required_analytical_tables() {
        let schema = schema_statements("analytics").join("\n");
        for table in [
            "fact_inventory_movements",
            "inventory_balance_current",
            "sku_location_minute_features",
            "sku_location_model_state",
            "reorder_decisions",
            "reorder_simulations",
            "reorder_proposals",
            "policy_decisions",
            "execution_attempts",
            "open_replenishments",
            "event_calendar",
            "supplier_disruption_signals",
        ] {
            assert!(schema.contains(table));
        }
        assert!(schema.contains("ReplacingMergeTree(version)"));
    }
}
