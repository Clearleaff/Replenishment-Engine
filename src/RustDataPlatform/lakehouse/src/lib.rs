use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Datelike, Timelike, Utc};
use data_platform_common::{InventoryMovementType, NormalizedEvent};
use polars::io::{SerReader, parquet::read::ParquetReader, parquet::write::ParquetWriter};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

pub const RAW_EVENT_SCHEMA_VERSION: &str = "raw-kafka-event-v1";
pub const SILVER_EVENT_SCHEMA_VERSION: &str = "silver-inventory-event-v1";
pub const GOLD_DAILY_DEMAND_SCHEMA_VERSION: &str = "gold-daily-demand-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaHeader {
    pub key: String,
    pub value_base64: Option<String>,
}

impl KafkaHeader {
    pub fn new(key: impl Into<String>, value: Option<&[u8]>) -> Self {
        Self {
            key: key.into(),
            value_base64: value.map(|bytes| STANDARD.encode(bytes)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawKafkaEvent {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub key_base64: Option<String>,
    pub value_base64: Option<String>,
    pub headers: Vec<KafkaHeader>,
    pub ingestion_timestamp: DateTime<Utc>,
    pub schema_version: String,
}

impl RawKafkaEvent {
    pub fn from_input(input: RawKafkaEventInput<'_>) -> Self {
        Self {
            topic: input.topic,
            partition: input.partition,
            offset: input.offset,
            event_timestamp: input.event_timestamp,
            key_base64: input.key.map(|bytes| STANDARD.encode(bytes)),
            value_base64: input.value.map(|bytes| STANDARD.encode(bytes)),
            headers: input.headers,
            ingestion_timestamp: input.ingestion_timestamp,
            schema_version: RAW_EVENT_SCHEMA_VERSION.to_owned(),
        }
    }

    pub fn identity(&self) -> KafkaEventIdentity {
        KafkaEventIdentity {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.offset,
        }
    }

    fn partition_time(&self) -> DateTime<Utc> {
        self.event_timestamp.unwrap_or(self.ingestion_timestamp)
    }
}

#[derive(Debug)]
pub struct RawKafkaEventInput<'a> {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Vec<KafkaHeader>,
    pub ingestion_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KafkaEventIdentity {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl KafkaEventIdentity {
    pub fn stable_id(&self) -> String {
        format!("{}:{}:{}", self.topic, self.partition, self.offset)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistOutcome {
    Written,
    AlreadyExists,
}

#[derive(Debug, Clone)]
pub struct LocalBronzeLake {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalSilverLake {
    root: PathBuf,
}

impl LocalSilverLake {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn event_path(&self, event: &SilverInventoryEvent) -> PathBuf {
        let at = event.occurred_at.unwrap_or_else(Utc::now);
        self.root
            .join("silver")
            .join("inventory_events")
            .join(format!(
                "date={:04}-{:02}-{:02}",
                at.year(),
                at.month(),
                at.day()
            ))
            .join(format!("hour={:02}", at.hour()))
            .join(format!(
                "part-{}-{}.parquet",
                event.source_partition, event.source_offset
            ))
    }

    pub fn persist(&self, event: &SilverInventoryEvent) -> Result<PersistOutcome> {
        let path = self.event_path(event);
        if path.exists() {
            return Ok(PersistOutcome::AlreadyExists);
        }
        let mut df = silver_events_to_dataframe(std::slice::from_ref(event))?;
        write_parquet_atomic(&mut df, &path)?;
        Ok(PersistOutcome::Written)
    }
}

impl LocalBronzeLake {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn event_path(&self, event: &RawKafkaEvent) -> PathBuf {
        let at = event.partition_time();
        self.root
            .join("bronze")
            .join("kafka")
            .join(sanitize_path_segment(&event.topic))
            .join(format!(
                "date={:04}-{:02}-{:02}",
                at.year(),
                at.month(),
                at.day()
            ))
            .join(format!("hour={:02}", at.hour()))
            .join(format!("part-{}-{}.parquet", event.partition, event.offset))
    }

    pub fn persist(&self, event: &RawKafkaEvent) -> Result<PersistOutcome> {
        let path = self.event_path(event);
        if path.exists() {
            return Ok(PersistOutcome::AlreadyExists);
        }

        let mut df = raw_events_to_dataframe(std::slice::from_ref(event))?;
        write_parquet_atomic(&mut df, &path)?;
        Ok(PersistOutcome::Written)
    }
}

pub fn raw_events_to_dataframe(events: &[RawKafkaEvent]) -> Result<DataFrame> {
    let topics: Vec<_> = events.iter().map(|event| event.topic.as_str()).collect();
    let partitions: Vec<_> = events.iter().map(|event| event.partition).collect();
    let offsets: Vec<_> = events.iter().map(|event| event.offset).collect();
    let event_timestamps: Vec<_> = events
        .iter()
        .map(|event| event.event_timestamp.map(|value| value.to_rfc3339()))
        .collect();
    let key_base64: Vec<_> = events
        .iter()
        .map(|event| event.key_base64.as_deref())
        .collect();
    let value_base64: Vec<_> = events
        .iter()
        .map(|event| event.value_base64.as_deref())
        .collect();
    let value_is_null: Vec<_> = events
        .iter()
        .map(|event| event.value_base64.is_none())
        .collect();
    let headers_json = events
        .iter()
        .map(|event| serde_json::to_string(&event.headers))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let ingestion_timestamps: Vec<_> = events
        .iter()
        .map(|event| event.ingestion_timestamp.to_rfc3339())
        .collect();
    let schema_versions: Vec<_> = events
        .iter()
        .map(|event| event.schema_version.as_str())
        .collect();

    DataFrame::new(
        events.len(),
        vec![
            Series::new("topic".into(), topics).into(),
            Series::new("partition".into(), partitions).into(),
            Series::new("offset".into(), offsets).into(),
            Series::new("event_timestamp".into(), event_timestamps).into(),
            Series::new("key_base64".into(), key_base64).into(),
            Series::new("value_base64".into(), value_base64).into(),
            Series::new("value_is_null".into(), value_is_null).into(),
            Series::new("headers_json".into(), headers_json).into(),
            Series::new("ingestion_timestamp".into(), ingestion_timestamps).into(),
            Series::new("schema_version".into(), schema_versions).into(),
        ],
    )
    .map_err(anyhow::Error::from)
    .context("failed to create raw event dataframe")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SilverInventoryEvent {
    pub event_id: String,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub cdc_operation: String,
    pub event_type: String,
    pub sku_id: Option<i32>,
    pub location_code: Option<String>,
    pub movement_type: Option<String>,
    pub quantity: Option<i32>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub balance_version: Option<i64>,
    pub on_hand: Option<i32>,
    pub reserved: Option<i32>,
    pub available: Option<i32>,
    pub schema_version: String,
}

impl SilverInventoryEvent {
    pub fn from_normalized(identity: &KafkaEventIdentity, event: &NormalizedEvent) -> Self {
        let event_id = identity.stable_id();
        match event {
            NormalizedEvent::Movement {
                operation, fact, ..
            } => Self {
                event_id,
                source_topic: identity.topic.clone(),
                source_partition: identity.partition,
                source_offset: identity.offset,
                cdc_operation: format!("{operation:?}").to_ascii_uppercase(),
                event_type: "INVENTORY_MOVEMENT".to_owned(),
                sku_id: Some(fact.sku_id),
                location_code: Some(fact.location_code.clone()),
                movement_type: Some(format!("{:?}", fact.movement_type)),
                quantity: Some(fact.quantity),
                occurred_at: Some(fact.occurred_at),
                balance_version: Some(fact.resulting_balance_version),
                on_hand: None,
                reserved: None,
                available: None,
                schema_version: SILVER_EVENT_SCHEMA_VERSION.to_owned(),
            },
            NormalizedEvent::Balance {
                operation, state, ..
            } => Self {
                event_id,
                source_topic: identity.topic.clone(),
                source_partition: identity.partition,
                source_offset: identity.offset,
                cdc_operation: format!("{operation:?}").to_ascii_uppercase(),
                event_type: "INVENTORY_BALANCE".to_owned(),
                sku_id: Some(state.sku_id),
                location_code: Some(state.location_code.clone()),
                movement_type: None,
                quantity: None,
                occurred_at: Some(state.updated_at),
                balance_version: Some(state.version),
                on_hand: Some(state.on_hand),
                reserved: Some(state.reserved),
                available: Some(state.available),
                schema_version: SILVER_EVENT_SCHEMA_VERSION.to_owned(),
            },
            NormalizedEvent::MovementDelete { movement_id, .. } => Self {
                event_id,
                source_topic: identity.topic.clone(),
                source_partition: identity.partition,
                source_offset: identity.offset,
                cdc_operation: "DELETE".to_owned(),
                event_type: "INVENTORY_MOVEMENT_DELETE".to_owned(),
                sku_id: None,
                location_code: None,
                movement_type: None,
                quantity: None,
                occurred_at: None,
                balance_version: None,
                on_hand: None,
                reserved: None,
                available: None,
                schema_version: format!("{SILVER_EVENT_SCHEMA_VERSION}:{movement_id}"),
            },
            NormalizedEvent::BalanceDelete { key, version, .. } => Self {
                event_id,
                source_topic: identity.topic.clone(),
                source_partition: identity.partition,
                source_offset: identity.offset,
                cdc_operation: "DELETE".to_owned(),
                event_type: "INVENTORY_BALANCE_DELETE".to_owned(),
                sku_id: Some(key.sku_id),
                location_code: Some(key.location_code.clone()),
                movement_type: None,
                quantity: None,
                occurred_at: None,
                balance_version: Some(*version),
                on_hand: None,
                reserved: None,
                available: None,
                schema_version: SILVER_EVENT_SCHEMA_VERSION.to_owned(),
            },
            NormalizedEvent::Tombstone { topic } => Self {
                event_id,
                source_topic: identity.topic.clone(),
                source_partition: identity.partition,
                source_offset: identity.offset,
                cdc_operation: "TOMBSTONE".to_owned(),
                event_type: format!("KAFKA_TOMBSTONE:{topic}"),
                sku_id: None,
                location_code: None,
                movement_type: None,
                quantity: None,
                occurred_at: None,
                balance_version: None,
                on_hand: None,
                reserved: None,
                available: None,
                schema_version: SILVER_EVENT_SCHEMA_VERSION.to_owned(),
            },
        }
    }
}

pub fn silver_events_to_dataframe(events: &[SilverInventoryEvent]) -> Result<DataFrame> {
    let event_id: Vec<_> = events.iter().map(|event| event.event_id.as_str()).collect();
    let source_topic: Vec<_> = events
        .iter()
        .map(|event| event.source_topic.as_str())
        .collect();
    let source_partition: Vec<_> = events.iter().map(|event| event.source_partition).collect();
    let source_offset: Vec<_> = events.iter().map(|event| event.source_offset).collect();
    let cdc_operation: Vec<_> = events
        .iter()
        .map(|event| event.cdc_operation.as_str())
        .collect();
    let event_type: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    let sku_id: Vec<_> = events.iter().map(|event| event.sku_id).collect();
    let location_code: Vec<_> = events
        .iter()
        .map(|event| event.location_code.as_deref())
        .collect();
    let movement_type: Vec<_> = events
        .iter()
        .map(|event| event.movement_type.as_deref())
        .collect();
    let quantity: Vec<_> = events.iter().map(|event| event.quantity).collect();
    let occurred_at: Vec<_> = events
        .iter()
        .map(|event| event.occurred_at.map(|value| value.to_rfc3339()))
        .collect();
    let balance_version: Vec<_> = events.iter().map(|event| event.balance_version).collect();
    let on_hand: Vec<_> = events.iter().map(|event| event.on_hand).collect();
    let reserved: Vec<_> = events.iter().map(|event| event.reserved).collect();
    let available: Vec<_> = events.iter().map(|event| event.available).collect();
    let schema_version: Vec<_> = events
        .iter()
        .map(|event| event.schema_version.as_str())
        .collect();

    DataFrame::new(
        events.len(),
        vec![
            Series::new("event_id".into(), event_id).into(),
            Series::new("source_topic".into(), source_topic).into(),
            Series::new("source_partition".into(), source_partition).into(),
            Series::new("source_offset".into(), source_offset).into(),
            Series::new("cdc_operation".into(), cdc_operation).into(),
            Series::new("event_type".into(), event_type).into(),
            Series::new("sku_id".into(), sku_id).into(),
            Series::new("location_code".into(), location_code).into(),
            Series::new("movement_type".into(), movement_type).into(),
            Series::new("quantity".into(), quantity).into(),
            Series::new("occurred_at".into(), occurred_at).into(),
            Series::new("balance_version".into(), balance_version).into(),
            Series::new("on_hand".into(), on_hand).into(),
            Series::new("reserved".into(), reserved).into(),
            Series::new("available".into(), available).into(),
            Series::new("schema_version".into(), schema_version).into(),
        ],
    )
    .map_err(anyhow::Error::from)
    .context("failed to create silver event dataframe")
}

pub fn read_silver_events_from_parquet(
    path: impl AsRef<Path>,
) -> Result<Vec<SilverInventoryEvent>> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let df = anyhow::Context::with_context(ParquetReader::new(file).finish(), || {
        format!("failed to read {}", path.display())
    })?;

    let event_id = df.column("event_id")?.str()?;
    let source_topic = df.column("source_topic")?.str()?;
    let source_partition = df.column("source_partition")?.i32()?;
    let source_offset = df.column("source_offset")?.i64()?;
    let cdc_operation = df.column("cdc_operation")?.str()?;
    let event_type = df.column("event_type")?.str()?;
    let sku_id = df.column("sku_id")?.i32()?;
    let location_code = df.column("location_code")?.str()?;
    let movement_type = df.column("movement_type")?.str()?;
    let quantity = df.column("quantity")?.i32()?;
    let occurred_at = df.column("occurred_at")?.str()?;
    let balance_version = df.column("balance_version")?.i64()?;
    let on_hand = df.column("on_hand")?.i32()?;
    let reserved = df.column("reserved")?.i32()?;
    let available = df.column("available")?.i32()?;
    let schema_version = df.column("schema_version")?.str()?;

    (0..df.height())
        .map(|index| {
            Ok(SilverInventoryEvent {
                event_id: required_string(event_id.get(index), "event_id")?,
                source_topic: required_string(source_topic.get(index), "source_topic")?,
                source_partition: source_partition
                    .get(index)
                    .context("source_partition is null")?,
                source_offset: source_offset.get(index).context("source_offset is null")?,
                cdc_operation: required_string(cdc_operation.get(index), "cdc_operation")?,
                event_type: required_string(event_type.get(index), "event_type")?,
                sku_id: sku_id.get(index),
                location_code: location_code.get(index).map(str::to_owned),
                movement_type: movement_type.get(index).map(str::to_owned),
                quantity: quantity.get(index),
                occurred_at: occurred_at
                    .get(index)
                    .map(|value| {
                        DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
                    })
                    .transpose()
                    .context("occurred_at is not RFC3339")?,
                balance_version: balance_version.get(index),
                on_hand: on_hand.get(index),
                reserved: reserved.get(index),
                available: available.get(index),
                schema_version: required_string(schema_version.get(index), "schema_version")?,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyDemand {
    pub sku_id: i32,
    pub location_code: String,
    pub date: String,
    pub weekday: String,
    pub sale_units: i32,
    pub movement_count: u32,
    pub schema_version: String,
}

pub fn daily_sales_gold(events: &[SilverInventoryEvent]) -> Vec<DailyDemand> {
    let mut groups: BTreeMap<(i32, String, String, String), (i32, u32)> = BTreeMap::new();
    for event in events {
        if event.movement_type.as_deref() != Some("Sale") {
            continue;
        }
        let (Some(sku_id), Some(location), Some(quantity), Some(occurred_at)) = (
            event.sku_id,
            event.location_code.as_ref(),
            event.quantity,
            event.occurred_at,
        ) else {
            continue;
        };
        let date = occurred_at.date_naive().to_string();
        let weekday = format!("{:?}", occurred_at.weekday()).to_ascii_uppercase();
        let entry = groups
            .entry((sku_id, location.clone(), date, weekday))
            .or_default();
        entry.0 += quantity;
        entry.1 += 1;
    }

    groups
        .into_iter()
        .map(
            |((sku_id, location_code, date, weekday), (sale_units, movement_count))| DailyDemand {
                sku_id,
                location_code,
                date,
                weekday,
                sale_units,
                movement_count,
                schema_version: GOLD_DAILY_DEMAND_SCHEMA_VERSION.to_owned(),
            },
        )
        .collect()
}

pub fn daily_demand_to_dataframe(rows: &[DailyDemand]) -> Result<DataFrame> {
    let sku_id: Vec<_> = rows.iter().map(|row| row.sku_id).collect();
    let location_code: Vec<_> = rows.iter().map(|row| row.location_code.as_str()).collect();
    let date: Vec<_> = rows.iter().map(|row| row.date.as_str()).collect();
    let weekday: Vec<_> = rows.iter().map(|row| row.weekday.as_str()).collect();
    let sale_units: Vec<_> = rows.iter().map(|row| row.sale_units).collect();
    let movement_count: Vec<_> = rows.iter().map(|row| row.movement_count).collect();
    let schema_version: Vec<_> = rows.iter().map(|row| row.schema_version.as_str()).collect();

    DataFrame::new(
        rows.len(),
        vec![
            Series::new("sku_id".into(), sku_id).into(),
            Series::new("location_code".into(), location_code).into(),
            Series::new("date".into(), date).into(),
            Series::new("weekday".into(), weekday).into(),
            Series::new("sale_units".into(), sale_units).into(),
            Series::new("movement_count".into(), movement_count).into(),
            Series::new("schema_version".into(), schema_version).into(),
        ],
    )
    .map_err(anyhow::Error::from)
    .context("failed to create daily demand dataframe")
}

pub fn write_parquet(mut df: DataFrame, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    write_parquet_atomic(&mut df, path)
}

fn write_parquet_atomic(df: &mut DataFrame, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("parquet.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    anyhow::Context::with_context(ParquetWriter::new(file).finish(df), || {
        format!("failed to write {}", path.display())
    })?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("failed to atomically publish {}", path.display()))?;
    Ok(())
}

fn sanitize_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '_',
        })
        .collect()
}

fn required_string(value: Option<&str>, column: &str) -> Result<String> {
    value
        .map(str::to_owned)
        .with_context(|| format!("{column} is null"))
}

pub fn movement_type_name(value: InventoryMovementType) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use polars::io::{SerReader, parquet::read::ParquetReader};

    use chrono::TimeZone;
    use data_platform_common::{ChangeOperation, InventoryMovementFact, MOVEMENT_TOPIC};
    use uuid::Uuid;

    use super::*;

    fn sample_raw(offset: i64) -> RawKafkaEvent {
        RawKafkaEvent::from_input(RawKafkaEventInput {
            topic: MOVEMENT_TOPIC.to_owned(),
            partition: 0,
            offset,
            event_timestamp: Some(Utc.with_ymd_and_hms(2026, 9, 13, 9, 30, 0).unwrap()),
            key: Some(br#"{"id":1}"#),
            value: Some(br#"{"payload":{"after":true}}"#),
            headers: vec![KafkaHeader::new("traceparent", Some(b"abc"))],
            ingestion_timestamp: Utc.with_ymd_and_hms(2026, 9, 13, 9, 30, 2).unwrap(),
        })
    }

    #[test]
    fn bronze_path_is_topic_date_hour_partitioned() {
        let lake = LocalBronzeLake::new("/tmp/lake");
        let path = lake.event_path(&sample_raw(17));
        assert!(path.ends_with(
            "bronze/kafka/eshop.inventory.inventory_movements/date=2026-09-13/hour=09/part-0-17.parquet"
        ));
    }

    #[test]
    fn bronze_persistence_is_idempotent_by_topic_partition_offset() {
        let temp = tempfile::tempdir().unwrap();
        let lake = LocalBronzeLake::new(temp.path());
        let event = sample_raw(18);

        assert_eq!(lake.persist(&event).unwrap(), PersistOutcome::Written);
        assert_eq!(lake.persist(&event).unwrap(), PersistOutcome::AlreadyExists);
        assert!(lake.event_path(&event).exists());
    }

    #[test]
    fn raw_dataframe_preserves_payload_and_tombstone_shape() {
        let raw = RawKafkaEvent::from_input(RawKafkaEventInput {
            topic: MOVEMENT_TOPIC.to_owned(),
            partition: 0,
            offset: 19,
            event_timestamp: None,
            key: None,
            value: None,
            headers: Vec::new(),
            ingestion_timestamp: Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap(),
        });
        let df = raw_events_to_dataframe(&[raw]).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("schema_version").unwrap().str().unwrap().get(0),
            Some(RAW_EVENT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn silver_and_gold_turn_sale_movements_into_daily_demand() {
        let fact = InventoryMovementFact {
            movement_id: Uuid::new_v4(),
            source_event_id: Uuid::new_v4(),
            sku_id: 20,
            location_code: "BLR".to_owned(),
            order_id: Some(7),
            movement_type: InventoryMovementType::Sale,
            quantity: 3,
            occurred_at: Utc.with_ymd_and_hms(2026, 9, 13, 11, 0, 0).unwrap(),
            recorded_at: Utc.with_ymd_and_hms(2026, 9, 13, 11, 0, 2).unwrap(),
            resulting_balance_version: 8,
            reason: None,
        };
        let event = NormalizedEvent::Movement {
            operation: ChangeOperation::Create,
            fact,
            source_lsn: Some(42),
        };
        let identity = KafkaEventIdentity {
            topic: MOVEMENT_TOPIC.to_owned(),
            partition: 0,
            offset: 20,
        };
        let silver = SilverInventoryEvent::from_normalized(&identity, &event);
        let gold = daily_sales_gold(&[silver]);

        assert_eq!(gold.len(), 1);
        assert_eq!(gold[0].sku_id, 20);
        assert_eq!(gold[0].location_code, "BLR");
        assert_eq!(gold[0].sale_units, 3);
        assert_eq!(gold[0].weekday, "SUN");
    }

    #[test]
    fn parquet_writer_creates_readable_silver_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("silver.parquet");
        let raw = sample_raw(21);
        write_parquet(raw_events_to_dataframe(&[raw]).unwrap(), &path).unwrap();
        let file = File::open(path).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn reads_silver_parquet_back_for_batch_gold_analytics() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("silver.parquet");
        let events = vec![SilverInventoryEvent {
            event_id: "topic:0:1".to_owned(),
            source_topic: MOVEMENT_TOPIC.to_owned(),
            source_partition: 0,
            source_offset: 1,
            cdc_operation: "CREATE".to_owned(),
            event_type: "INVENTORY_MOVEMENT".to_owned(),
            sku_id: Some(20),
            location_code: Some("BLR".to_owned()),
            movement_type: Some("Sale".to_owned()),
            quantity: Some(5),
            occurred_at: Some(Utc.with_ymd_and_hms(2026, 9, 13, 13, 0, 0).unwrap()),
            balance_version: Some(4),
            on_hand: None,
            reserved: None,
            available: None,
            schema_version: SILVER_EVENT_SCHEMA_VERSION.to_owned(),
        }];
        write_parquet(silver_events_to_dataframe(&events).unwrap(), &path).unwrap();

        let recovered = read_silver_events_from_parquet(&path).unwrap();
        let gold = daily_sales_gold(&recovered);

        assert_eq!(recovered, events);
        assert_eq!(gold[0].sale_units, 5);
    }

    #[test]
    fn silver_persistence_is_idempotent_and_partitioned() {
        let temp = tempfile::tempdir().unwrap();
        let lake = LocalSilverLake::new(temp.path());
        let identity = KafkaEventIdentity {
            topic: MOVEMENT_TOPIC.to_owned(),
            partition: 0,
            offset: 22,
        };
        let fact = InventoryMovementFact {
            movement_id: Uuid::new_v4(),
            source_event_id: Uuid::new_v4(),
            sku_id: 20,
            location_code: "BLR".to_owned(),
            order_id: Some(9),
            movement_type: InventoryMovementType::Sale,
            quantity: 2,
            occurred_at: Utc.with_ymd_and_hms(2026, 9, 13, 12, 0, 0).unwrap(),
            recorded_at: Utc.with_ymd_and_hms(2026, 9, 13, 12, 0, 1).unwrap(),
            resulting_balance_version: 3,
            reason: None,
        };
        let event = NormalizedEvent::Movement {
            operation: ChangeOperation::Create,
            fact,
            source_lsn: None,
        };
        let silver = SilverInventoryEvent::from_normalized(&identity, &event);

        assert_eq!(lake.persist(&silver).unwrap(), PersistOutcome::Written);
        assert_eq!(
            lake.persist(&silver).unwrap(),
            PersistOutcome::AlreadyExists
        );
        assert!(
            lake.event_path(&silver)
                .ends_with("silver/inventory_events/date=2026-09-13/hour=12/part-0-22.parquet")
        );
    }
}
