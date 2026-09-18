use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MOVEMENT_TOPIC: &str = "eshop.inventory.inventory_movements";
pub const BALANCE_TOPIC: &str = "eshop.inventory.inventory_balances";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SkuLocation {
    pub sku_id: i32,
    pub location_code: String,
}

impl SkuLocation {
    pub fn new(sku_id: i32, location_code: impl Into<String>) -> Self {
        Self {
            sku_id,
            location_code: location_code.into().trim().to_ascii_uppercase(),
        }
    }

    pub fn stream_key(&self) -> String {
        format!("{}:{}", self.sku_id, self.location_code)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    Create,
    Update,
    Delete,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryMovementType {
    Reserve,
    Release,
    Sale,
    Restock,
    Return,
    Adjustment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryMovementFact {
    pub movement_id: Uuid,
    pub source_event_id: Uuid,
    pub sku_id: i32,
    pub location_code: String,
    pub order_id: Option<i32>,
    pub movement_type: InventoryMovementType,
    pub quantity: i32,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
    pub resulting_balance_version: i64,
    pub reason: Option<String>,
}

impl InventoryMovementFact {
    pub fn sku_location(&self) -> SkuLocation {
        SkuLocation::new(self.sku_id, &self.location_code)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryBalanceState {
    pub sku_id: i32,
    pub location_code: String,
    pub on_hand: i32,
    pub reserved: i32,
    pub available: i32,
    pub safety_stock: i32,
    pub reorder_point: i32,
    pub max_stock: i32,
    pub version: i64,
    pub updated_at: DateTime<Utc>,
}

impl InventoryBalanceState {
    pub fn sku_location(&self) -> SkuLocation {
        SkuLocation::new(self.sku_id, &self.location_code)
    }

    pub fn invariants_hold(&self) -> bool {
        self.reserved >= 0
            && self.reserved <= self.on_hand
            && self.on_hand <= self.max_stock
            && self.safety_stock >= 0
            && self.safety_stock <= self.reorder_point
            && self.reorder_point <= self.max_stock
            && self.available == self.on_hand - self.reserved
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NormalizedEvent {
    Movement {
        operation: ChangeOperation,
        fact: InventoryMovementFact,
        source_lsn: Option<i64>,
    },
    MovementDelete {
        movement_id: Uuid,
        source_lsn: Option<i64>,
    },
    Balance {
        operation: ChangeOperation,
        state: InventoryBalanceState,
        source_lsn: Option<i64>,
    },
    BalanceDelete {
        key: SkuLocation,
        version: i64,
        source_lsn: Option<i64>,
    },
    Tombstone {
        topic: String,
    },
}
