use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use warehouse::{
    AnalyticalWarehouse, ClickHouseWarehouse, EventCalendarEntry, SupplierSignalEntry,
};

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    data: T,
    fetched_at: Instant,
}

type CalendarMap = HashMap<(String, u32), CachedEntry<Vec<EventCalendarEntry>>>;
type SignalMap = HashMap<(Option<i32>, String), CachedEntry<Vec<SupplierSignalEntry>>>;

#[derive(Debug)]
pub struct MacroSignalCache {
    ttl: Duration,
    events: RwLock<CalendarMap>,
    signals: RwLock<SignalMap>,
}

impl MacroSignalCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            events: RwLock::new(HashMap::new()),
            signals: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_event_calendar(
        &self,
        location_code: &str,
        days_ahead: u32,
        warehouse: &ClickHouseWarehouse,
    ) -> anyhow::Result<Vec<EventCalendarEntry>> {
        let key = (location_code.trim().to_ascii_uppercase(), days_ahead);
        {
            let read = self.events.read().await;
            if let Some(entry) = read.get(&key).filter(|e| e.fetched_at.elapsed() < self.ttl) {
                return Ok(entry.data.clone());
            }
        }

        let fresh = warehouse
            .query_event_calendar(location_code, days_ahead)
            .await?;
        let mut write = self.events.write().await;
        write.insert(
            key,
            CachedEntry {
                data: fresh.clone(),
                fetched_at: Instant::now(),
            },
        );
        Ok(fresh)
    }

    pub async fn get_supplier_signals(
        &self,
        sku_id: Option<i32>,
        location_code: &str,
        warehouse: &ClickHouseWarehouse,
    ) -> anyhow::Result<Vec<SupplierSignalEntry>> {
        let key = (sku_id, location_code.trim().to_ascii_uppercase());
        {
            let read = self.signals.read().await;
            if let Some(entry) = read.get(&key).filter(|e| e.fetched_at.elapsed() < self.ttl) {
                return Ok(entry.data.clone());
            }
        }

        let fresh = warehouse
            .query_supplier_signals(sku_id, location_code)
            .await?;
        let mut write = self.signals.write().await;
        write.insert(
            key,
            CachedEntry {
                data: fresh.clone(),
                fetched_at: Instant::now(),
            },
        );
        Ok(fresh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_hits_memory_when_populated() {
        let cache = MacroSignalCache::new(Duration::from_secs(60));
        let key = ("NCR".to_owned(), 30);
        {
            let mut write = cache.events.write().await;
            write.insert(
                key.clone(),
                CachedEntry {
                    data: Vec::new(),
                    fetched_at: Instant::now(),
                },
            );
        }

        // Even with a dummy/unreachable warehouse, get_event_calendar succeeds by returning cached data!
        let bad_ch = ClickHouseWarehouse::new(warehouse::ClickHouseConfig {
            url: "http://127.0.0.1:1".to_owned(),
            database: "test".to_owned(),
            user: "test".to_owned(),
            password: "test".to_owned(),
        });

        let result = cache.get_event_calendar("NCR", 30, &bad_ch).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn signals_cache_hits_memory_when_populated() {
        let cache = MacroSignalCache::new(Duration::from_secs(60));
        let key = (Some(42), "NCR".to_owned());
        {
            let mut write = cache.signals.write().await;
            write.insert(
                key.clone(),
                CachedEntry {
                    data: Vec::new(),
                    fetched_at: Instant::now(),
                },
            );
        }

        let bad_ch = ClickHouseWarehouse::new(warehouse::ClickHouseConfig {
            url: "http://127.0.0.1:1".to_owned(),
            database: "test".to_owned(),
            user: "test".to_owned(),
            password: "test".to_owned(),
        });

        let result = cache.get_supplier_signals(Some(42), "NCR", &bad_ch).await;
        assert!(result.is_ok());
    }
}
