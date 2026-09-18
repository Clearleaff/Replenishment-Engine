use chrono::{DateTime, Utc};
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

use crate::model::{
    ChangeOperation, InventoryBalanceState, InventoryMovementFact, InventoryMovementType,
    NormalizedEvent, SkuLocation,
};

#[derive(Debug, Error, PartialEq)]
pub enum DecodeError {
    #[error("malformed Debezium JSON: {0}")]
    MalformedJson(String),
    #[error("unsupported Debezium operation: {0}")]
    UnsupportedOperation(String),
    #[error("unsupported source table: {0}")]
    UnsupportedTable(String),
    #[error("operation {operation} for {table} has no required row image")]
    MissingRow { operation: String, table: String },
    #[error("inventory movement updates are quarantined because facts are immutable")]
    UnexpectedMovementUpdate,
    #[error("invalid inventory balance invariants for {0}")]
    InvalidBalance(String),
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    before: Option<T>,
    after: Option<T>,
    source: Source,
    op: String,
}

#[derive(Debug, Deserialize)]
struct Source {
    table: String,
    #[serde(default)]
    lsn: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct MovementRow {
    movement_id: Uuid,
    source_event_id: Uuid,
    sku_id: i32,
    location_code: String,
    order_id: Option<i32>,
    movement_type: InventoryMovementType,
    quantity: i32,
    occurred_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    balance_version_after: i64,
    reason: Option<String>,
}

impl From<MovementRow> for InventoryMovementFact {
    fn from(row: MovementRow) -> Self {
        Self {
            movement_id: row.movement_id,
            source_event_id: row.source_event_id,
            sku_id: row.sku_id,
            location_code: row.location_code,
            order_id: row.order_id,
            movement_type: row.movement_type,
            quantity: row.quantity,
            occurred_at: row.occurred_at,
            recorded_at: row.recorded_at,
            resulting_balance_version: row.balance_version_after,
            reason: row.reason,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BalanceRow {
    sku_id: i32,
    location_code: String,
    on_hand: i32,
    reserved: i32,
    safety_stock: i32,
    reorder_point: i32,
    max_stock: i32,
    version: i64,
    updated_at: DateTime<Utc>,
}

impl From<BalanceRow> for InventoryBalanceState {
    fn from(row: BalanceRow) -> Self {
        Self {
            sku_id: row.sku_id,
            location_code: row.location_code,
            on_hand: row.on_hand,
            reserved: row.reserved,
            available: row.on_hand - row.reserved,
            safety_stock: row.safety_stock,
            reorder_point: row.reorder_point,
            max_stock: row.max_stock,
            version: row.version,
            updated_at: row.updated_at,
        }
    }
}

pub struct DebeziumDecoder;

impl DebeziumDecoder {
    pub fn decode(topic: &str, payload: Option<&[u8]>) -> Result<NormalizedEvent, DecodeError> {
        let Some(payload) = payload else {
            return Ok(NormalizedEvent::Tombstone {
                topic: topic.to_owned(),
            });
        };

        let value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|error| DecodeError::MalformedJson(error.to_string()))?;
        let value = value.get("payload").cloned().unwrap_or(value);
        let table = value
            .get("source")
            .and_then(|source| source.get("table"))
            .and_then(|table| table.as_str())
            .ok_or_else(|| DecodeError::MalformedJson("source.table is required".to_owned()))?;

        match table {
            "inventory_movements" => Self::decode_movement(value),
            "inventory_balances" => Self::decode_balance(value),
            other => Err(DecodeError::UnsupportedTable(other.to_owned())),
        }
    }

    fn decode_movement(value: serde_json::Value) -> Result<NormalizedEvent, DecodeError> {
        let envelope: Envelope<MovementRow> = serde_json::from_value(value)
            .map_err(|error| DecodeError::MalformedJson(error.to_string()))?;
        let operation = parse_operation(&envelope.op)?;

        match operation {
            ChangeOperation::Create | ChangeOperation::Snapshot => {
                let row = envelope.after.ok_or(DecodeError::MissingRow {
                    operation: envelope.op,
                    table: envelope.source.table,
                })?;
                Ok(NormalizedEvent::Movement {
                    operation,
                    fact: row.into(),
                    source_lsn: envelope.source.lsn,
                })
            }
            ChangeOperation::Update => Err(DecodeError::UnexpectedMovementUpdate),
            ChangeOperation::Delete => {
                let row = envelope.before.ok_or(DecodeError::MissingRow {
                    operation: envelope.op,
                    table: envelope.source.table,
                })?;
                Ok(NormalizedEvent::MovementDelete {
                    movement_id: row.movement_id,
                    source_lsn: envelope.source.lsn,
                })
            }
        }
    }

    fn decode_balance(value: serde_json::Value) -> Result<NormalizedEvent, DecodeError> {
        let envelope: Envelope<BalanceRow> = serde_json::from_value(value)
            .map_err(|error| DecodeError::MalformedJson(error.to_string()))?;
        let operation = parse_operation(&envelope.op)?;

        match operation {
            ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Snapshot => {
                let row = envelope.after.ok_or(DecodeError::MissingRow {
                    operation: envelope.op,
                    table: envelope.source.table,
                })?;
                let state: InventoryBalanceState = row.into();
                if !state.invariants_hold() {
                    return Err(DecodeError::InvalidBalance(
                        state.sku_location().stream_key(),
                    ));
                }
                Ok(NormalizedEvent::Balance {
                    operation,
                    state,
                    source_lsn: envelope.source.lsn,
                })
            }
            ChangeOperation::Delete => {
                let row = envelope.before.ok_or(DecodeError::MissingRow {
                    operation: envelope.op,
                    table: envelope.source.table,
                })?;
                Ok(NormalizedEvent::BalanceDelete {
                    key: SkuLocation::new(row.sku_id, row.location_code),
                    version: row.version,
                    source_lsn: envelope.source.lsn,
                })
            }
        }
    }
}

fn parse_operation(value: &str) -> Result<ChangeOperation, DecodeError> {
    match value {
        "c" => Ok(ChangeOperation::Create),
        "u" => Ok(ChangeOperation::Update),
        "d" => Ok(ChangeOperation::Delete),
        "r" => Ok(ChangeOperation::Snapshot),
        other => Err(DecodeError::UnsupportedOperation(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BALANCE_TOPIC, MOVEMENT_TOPIC};

    #[test]
    fn decodes_movement_insert_and_ignores_unknown_fields() {
        let json = br#"{
          "before":null,
          "after":{"movement_id":"d8adbe37-c24e-4b51-b7c7-50d98e45ada1","source_event_id":"bd288d46-2f80-470a-85f4-7b65c4825388","sku_id":42,"location_code":"NCR","order_id":9,"movement_type":"Sale","quantity":2,"occurred_at":"2026-09-10T10:11:45.123456Z","recorded_at":"2026-09-10T10:11:45.130000Z","balance_version_after":18,"reason":null,"future_field":true},
          "source":{"table":"inventory_movements","lsn":123},"op":"c","ts_ms":1
        }"#;

        let event = DebeziumDecoder::decode(MOVEMENT_TOPIC, Some(json)).unwrap();
        match event {
            NormalizedEvent::Movement {
                fact, source_lsn, ..
            } => {
                assert_eq!(fact.sku_location().stream_key(), "42:NCR");
                assert_eq!(fact.movement_type, InventoryMovementType::Sale);
                assert_eq!(source_lsn, Some(123));
            }
            _ => panic!("expected movement"),
        }
    }

    #[test]
    fn decodes_balance_snapshot_and_calculates_available() {
        let json = br#"{
          "before":null,
          "after":{"sku_id":42,"location_code":"ncr","on_hand":98,"reserved":3,"safety_stock":20,"reorder_point":40,"max_stock":200,"version":18,"updated_at":"2026-09-10T10:11:45Z"},
          "source":{"table":"inventory_balances","lsn":124},"op":"r"
        }"#;

        let event = DebeziumDecoder::decode(BALANCE_TOPIC, Some(json)).unwrap();
        match event {
            NormalizedEvent::Balance {
                state, operation, ..
            } => {
                assert_eq!(state.available, 95);
                assert_eq!(state.sku_location().stream_key(), "42:NCR");
                assert_eq!(operation, ChangeOperation::Snapshot);
            }
            _ => panic!("expected balance"),
        }
    }

    #[test]
    fn decodes_kafka_connect_schema_wrapped_payload() {
        let json = br#"{
          "schema":{"type":"struct","name":"eshop.inventory.inventory_balances.Envelope"},
          "payload":{
            "before":null,
            "after":{"sku_id":42,"location_code":"NCR","on_hand":98,"reserved":3,"safety_stock":20,"reorder_point":40,"max_stock":200,"version":18,"updated_at":"2026-09-10T10:11:45Z"},
            "source":{"table":"inventory_balances","lsn":125},"op":"r"
          }
        }"#;

        let event = DebeziumDecoder::decode(BALANCE_TOPIC, Some(json)).unwrap();
        assert!(matches!(
            event,
            NormalizedEvent::Balance {
                state: InventoryBalanceState { available: 95, .. },
                operation: ChangeOperation::Snapshot,
                source_lsn: Some(125)
            }
        ));
    }

    #[test]
    fn rejects_malformed_and_immutable_movement_update() {
        assert!(matches!(
            DebeziumDecoder::decode(MOVEMENT_TOPIC, Some(b"not-json")),
            Err(DecodeError::MalformedJson(_))
        ));

        let json = br#"{
          "before":null,
          "after":{"movement_id":"d8adbe37-c24e-4b51-b7c7-50d98e45ada1","source_event_id":"bd288d46-2f80-470a-85f4-7b65c4825388","sku_id":42,"location_code":"NCR","order_id":9,"movement_type":"Sale","quantity":2,"occurred_at":"2026-09-10T10:11:45Z","recorded_at":"2026-09-10T10:11:45Z","balance_version_after":18,"reason":null},
          "source":{"table":"inventory_movements"},"op":"u"
        }"#;
        assert_eq!(
            DebeziumDecoder::decode(MOVEMENT_TOPIC, Some(json)),
            Err(DecodeError::UnexpectedMovementUpdate)
        );
    }

    #[test]
    fn handles_tombstone_without_guessing_business_effect() {
        assert_eq!(
            DebeziumDecoder::decode(MOVEMENT_TOPIC, None).unwrap(),
            NormalizedEvent::Tombstone {
                topic: MOVEMENT_TOPIC.to_owned()
            }
        );
    }
}
