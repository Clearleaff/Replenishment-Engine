use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc, Weekday};
use data_platform_common::{
    InventoryBalanceState, InventoryMovementFact, InventoryMovementType, SkuLocation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecayedStat {
    pub mean: f64,
    pub variance: f64,
    pub observation_count: u64,
}

impl Default for DecayedStat {
    fn default() -> Self {
        Self {
            mean: 0.0,
            variance: 0.0,
            observation_count: 0,
        }
    }
}

impl DecayedStat {
    pub fn observe(&mut self, value: f64, alpha: f64) {
        if self.observation_count == 0 {
            self.mean = value;
            self.variance = 0.0;
        } else {
            let delta = value - self.mean;
            self.mean += alpha * delta;
            self.variance = (1.0 - alpha) * (self.variance + alpha * delta * delta);
        }
        self.observation_count += 1;
    }

    pub fn stddev(&self) -> f64 {
        self.variance.max(0.0).sqrt()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OnlineDemandModel {
    weekday: Vec<DecayedStat>,
    hour: Vec<DecayedStat>,
    pub recent_rate_ewma: f64,
    pub observation_count: u64,
    pub last_update: Option<DateTime<Utc>>,
    decay_alpha: f64,
}

impl Default for OnlineDemandModel {
    fn default() -> Self {
        Self {
            weekday: vec![DecayedStat::default(); 7],
            hour: vec![DecayedStat::default(); 24],
            recent_rate_ewma: 0.0,
            observation_count: 0,
            last_update: None,
            decay_alpha: 0.2,
        }
    }
}

impl OnlineDemandModel {
    pub fn observe_daily_total(&mut self, weekday: Weekday, units: f64, at: DateTime<Utc>) {
        self.weekday[weekday.num_days_from_monday() as usize].observe(units, self.decay_alpha);
        self.observation_count += 1;
        self.last_update = Some(at);
    }

    pub fn observe_hourly_total(&mut self, hour: u32, units: f64, at: DateTime<Utc>) {
        self.hour[(hour % 24) as usize].observe(units, self.decay_alpha);
        self.observation_count += 1;
        self.last_update = Some(at);
    }

    pub fn observe_recent_rate(&mut self, rate_units_per_day: f64, at: DateTime<Utc>) {
        if self.recent_rate_ewma == 0.0 {
            self.recent_rate_ewma = rate_units_per_day;
        } else {
            self.recent_rate_ewma =
                0.35 * rate_units_per_day + (1.0 - 0.35) * self.recent_rate_ewma;
        }
        self.last_update = Some(at);
    }

    pub fn baseline(&self, at: DateTime<Utc>) -> f64 {
        let weekday_stat = &self.weekday[at.weekday().num_days_from_monday() as usize];
        let hourly_stat = &self.hour[at.hour() as usize];
        if weekday_stat.observation_count > 0 && hourly_stat.observation_count > 0 {
            0.65 * weekday_stat.mean + 0.35 * (hourly_stat.mean * 24.0)
        } else if weekday_stat.observation_count > 0 {
            weekday_stat.mean
        } else if hourly_stat.observation_count > 0 {
            hourly_stat.mean * 24.0
        } else {
            12.0
        }
    }

    pub fn forecast(
        &self,
        at: DateTime<Utc>,
        velocity_5m: f64,
        velocity_15m: f64,
        velocity_1h: f64,
    ) -> AdaptiveForecast {
        let weekday = &self.weekday[at.weekday().num_days_from_monday() as usize];
        let baseline = self.baseline(at).max(0.0);
        let live_recent = 0.55 * velocity_5m + 0.30 * velocity_15m + 0.15 * velocity_1h;
        let recent = if live_recent > 0.0 {
            live_recent
        } else {
            self.recent_rate_ewma
        };
        let epsilon = 0.1;
        let spike_ratio = recent / baseline.max(epsilon);
        let spike_score = ((recent - baseline) / weekday.stddev().max(1.0)).max(0.0);
        let recent_weight = if recent <= 0.0 {
            0.0
        } else {
            (0.2 + (spike_ratio - 1.25).max(0.0) * 0.2).clamp(0.2, 0.85)
        };
        let adaptive = (1.0 - recent_weight) * baseline + recent_weight * recent;

        AdaptiveForecast {
            baseline_units_per_day: baseline,
            recent_units_per_day: recent,
            forecast_units_per_day: adaptive.max(0.0),
            historical_stddev_units_per_day: weekday.stddev(),
            recent_vs_baseline_ratio: spike_ratio,
            spike_score,
            spike_detected: spike_ratio >= 2.0 && spike_score >= 2.0,
        }
    }

    pub fn forecast_horizon(
        &self,
        start: DateTime<Utc>,
        days: usize,
        velocity_5m: f64,
        velocity_15m: f64,
        velocity_1h: f64,
    ) -> Vec<DailyForecastPoint> {
        (0..days)
            .map(|day| {
                let at = start + Duration::days(day as i64);
                let forecast = self.forecast(at, velocity_5m, velocity_15m, velocity_1h);
                DailyForecastPoint {
                    date: at.date_naive().to_string(),
                    weekday: format!("{:?}", at.weekday()).to_ascii_uppercase(),
                    forecast_units: forecast.forecast_units_per_day,
                    baseline_units: forecast.baseline_units_per_day,
                    recent_units: forecast.recent_units_per_day,
                    spike_detected: forecast.spike_detected,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdaptiveForecast {
    pub baseline_units_per_day: f64,
    pub recent_units_per_day: f64,
    pub forecast_units_per_day: f64,
    pub historical_stddev_units_per_day: f64,
    pub recent_vs_baseline_ratio: f64,
    pub spike_score: f64,
    pub spike_detected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyForecastPoint {
    pub date: String,
    pub weekday: String,
    pub forecast_units: f64,
    pub baseline_units: f64,
    pub recent_units: f64,
    pub spike_detected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkuLocationFeatures {
    pub key: SkuLocation,
    pub computed_at: DateTime<Utc>,
    pub on_hand: i32,
    pub reserved: i32,
    pub available: i32,
    pub balance_version: i64,
    pub reserve_units_5m: i32,
    pub sale_units_5m: i32,
    pub sale_units_15m: i32,
    pub sale_units_1h: i32,
    pub sale_velocity_5m: f64,
    pub sale_velocity_15m: f64,
    pub sale_velocity_1h: f64,
    pub daily_sales: i32,
    pub forecast: AdaptiveForecast,
    pub forecast_horizon_days: Vec<DailyForecastPoint>,
}

#[derive(Debug, Clone)]
struct TimedUnits {
    at: DateTime<Utc>,
    quantity: i32,
}

/// Duration for which movement IDs are retained for dedup.
/// 10 minutes matches realistic AMQP redelivery and backoff ceilings.
const MOVEMENT_DEDUP_WINDOW_MINUTES: i64 = 10;

/// Hard cap on movement dedup buffer size per SKU-location.
/// Even for extremely hot SKUs during a festive spike (e.g. 5,000 sales/min),
/// this strictly caps heap growth to at most 500 * 32B = 16 KB per pair.
const MOVEMENT_DEDUP_MAX_ENTRIES: usize = 500;

/// Maximum number of day-slots tracked (8-day sliding window).
const DAILY_SLOTS: usize = 8;

/// Maximum number of hour-slots tracked (8 days × 24 hours).
const HOURLY_SLOTS: usize = DAILY_SLOTS * 24;

/// Time-windowed movement dedup entry: (occurred_at, movement_id).
#[derive(Debug, Clone)]
struct TimedMovementId {
    at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Debug, Clone)]
pub struct SkuLocationState {
    pub balance: Option<InventoryBalanceState>,
    pub model: OnlineDemandModel,
    /// Bounded dedup: retains movement IDs seen in the last 10 minutes,
    /// capped at MOVEMENT_DEDUP_MAX_ENTRIES (500).
    seen_movements: VecDeque<TimedMovementId>,
    sales: VecDeque<TimedUnits>,
    reserves: VecDeque<TimedUnits>,
    /// Fixed-size ring for daily sale totals (8-day window).
    daily_sale_totals: [i32; DAILY_SLOTS],
    daily_base_date: Option<NaiveDate>,
    /// Fixed-size ring for hourly sale totals (192 hours).
    hourly_sale_totals: [i32; HOURLY_SLOTS],
    /// Bitmask: 1 bit per daily slot (8 bits = 1 byte).
    finalized_daily_mask: u8,
    /// Bitmask: 1 bit per hourly slot (192 bits = 24 bytes).
    finalized_hourly_mask: [u8; 24],
    last_recent_rate_observation_at: Option<DateTime<Utc>>,
}

impl Default for SkuLocationState {
    fn default() -> Self {
        Self {
            balance: None,
            model: OnlineDemandModel::default(),
            seen_movements: VecDeque::new(),
            sales: VecDeque::new(),
            reserves: VecDeque::new(),
            daily_sale_totals: [0; DAILY_SLOTS],
            daily_base_date: None,
            hourly_sale_totals: [0; HOURLY_SLOTS],
            finalized_daily_mask: 0,
            finalized_hourly_mask: [0; 24],
            last_recent_rate_observation_at: None,
        }
    }
}

impl SkuLocationState {
    pub fn apply_balance(&mut self, state: InventoryBalanceState) -> bool {
        if self
            .balance
            .as_ref()
            .is_some_and(|stored| state.version <= stored.version)
        {
            return false;
        }
        self.balance = Some(state);
        true
    }

    pub fn apply_balance_snapshot(&mut self, state: InventoryBalanceState) {
        self.balance = Some(state);
    }

    pub fn seen_movements_len(&self) -> usize {
        self.seen_movements.len()
    }

    pub fn apply_movement(&mut self, fact: &InventoryMovementFact) -> bool {
        // 1. Evict dedup entries older than the 10-minute time window
        let dedup_cutoff = fact.occurred_at - Duration::minutes(MOVEMENT_DEDUP_WINDOW_MINUTES);
        while self
            .seen_movements
            .front()
            .is_some_and(|entry| entry.at < dedup_cutoff)
        {
            self.seen_movements.pop_front();
        }

        // 2. Enforce hard entry cap: prevent hot SKU festive spikes from growing memory unbounded
        while self.seen_movements.len() >= MOVEMENT_DEDUP_MAX_ENTRIES {
            self.seen_movements.pop_front();
        }

        // 3. Check for duplicate within the bounded window
        if self
            .seen_movements
            .iter()
            .any(|entry| entry.id == fact.movement_id)
        {
            return false;
        }

        // 4. Record movement ID
        self.seen_movements.push_back(TimedMovementId {
            at: fact.occurred_at,
            id: fact.movement_id,
        });

        let units = TimedUnits {
            at: fact.occurred_at,
            quantity: fact.quantity,
        };
        match fact.movement_type {
            InventoryMovementType::Sale => {
                insert_event_time_ordered(&mut self.sales, units);
                let date = fact.occurred_at.date_naive();
                let hour = fact.occurred_at.hour();
                let day_slot = date.num_days_from_ce() as usize % DAILY_SLOTS;
                let hour_slot = (day_slot * 24 + hour as usize) % HOURLY_SLOTS;

                // Reset slot if the date has wrapped around
                self.ensure_daily_slot_fresh(date, day_slot);

                self.daily_sale_totals[day_slot] += fact.quantity;
                self.hourly_sale_totals[hour_slot] += fact.quantity;
            }
            InventoryMovementType::Reserve => insert_event_time_ordered(&mut self.reserves, units),
            _ => {}
        }
        true
    }

    /// Ensure a daily slot is fresh for the given date. If the slot
    /// was last used for a different date (wrap-around), zero it and
    /// clear finalization flags.
    fn ensure_daily_slot_fresh(&mut self, date: NaiveDate, _day_slot: usize) {
        if let Some(base) = self.daily_base_date {
            let days_since_base = (date - base).num_days();
            if days_since_base >= DAILY_SLOTS as i64 {
                // Window has advanced past all slots; full reset
                self.daily_sale_totals = [0; DAILY_SLOTS];
                self.hourly_sale_totals = [0; HOURLY_SLOTS];
                self.finalized_daily_mask = 0;
                self.finalized_hourly_mask = [0; 24];
                self.daily_base_date = Some(date);
            }
        } else {
            self.daily_base_date = Some(date);
        }
    }

    pub fn calculate(&mut self, as_of: DateTime<Utc>) -> Option<SkuLocationFeatures> {
        let balance = self.balance.clone()?;
        let retention_start = as_of - Duration::days(8);
        while self
            .sales
            .front()
            .is_some_and(|entry| entry.at < retention_start)
        {
            self.sales.pop_front();
        }
        while self
            .reserves
            .front()
            .is_some_and(|entry| entry.at < retention_start)
        {
            self.reserves.pop_front();
        }

        let sale_5m = sum_since(&self.sales, as_of - Duration::minutes(5), as_of);
        let sale_15m = sum_since(&self.sales, as_of - Duration::minutes(15), as_of);
        let sale_1h = sum_since(&self.sales, as_of - Duration::hours(1), as_of);
        let reserve_5m = sum_since(&self.reserves, as_of - Duration::minutes(5), as_of);
        let day_start = as_of.date_naive().and_hms_opt(0, 0, 0)?.and_utc();
        let daily_sales = sum_since(&self.sales, day_start, as_of);
        let velocity_5m = sale_5m as f64 * 288.0;
        let velocity_15m = sale_15m as f64 * 96.0;
        let velocity_1h = sale_1h as f64 * 24.0;
        let recent = 0.55 * velocity_5m + 0.30 * velocity_15m + 0.15 * velocity_1h;
        self.observe_recent_rate_once_per_minute(recent, as_of);
        self.observe_finalized_history_buckets(as_of);
        let forecast = self
            .model
            .forecast(as_of, velocity_5m, velocity_15m, velocity_1h);
        let forecast_horizon_days =
            self.model
                .forecast_horizon(as_of, 7, velocity_5m, velocity_15m, velocity_1h);

        Some(SkuLocationFeatures {
            key: balance.sku_location(),
            computed_at: as_of,
            on_hand: balance.on_hand,
            reserved: balance.reserved,
            available: balance.available,
            balance_version: balance.version,
            reserve_units_5m: reserve_5m,
            sale_units_5m: sale_5m,
            sale_units_15m: sale_15m,
            sale_units_1h: sale_1h,
            sale_velocity_5m: velocity_5m,
            sale_velocity_15m: velocity_15m,
            sale_velocity_1h: velocity_1h,
            daily_sales,
            forecast,
            forecast_horizon_days,
        })
    }

    fn observe_recent_rate_once_per_minute(&mut self, recent: f64, as_of: DateTime<Utc>) {
        if self
            .last_recent_rate_observation_at
            .is_some_and(|last| as_of - last < Duration::minutes(1))
        {
            return;
        }

        self.model.observe_recent_rate(recent, as_of);
        self.last_recent_rate_observation_at = Some(as_of);
    }

    fn observe_finalized_history_buckets(&mut self, as_of: DateTime<Utc>) {
        let current_date = as_of.date_naive();

        // Finalize daily buckets from the fixed-size array
        for offset in 0..DAILY_SLOTS {
            let day_slot = offset;
            let bit = 1u8 << day_slot;
            // Skip if already finalized or zero total
            if self.finalized_daily_mask & bit != 0 {
                continue;
            }
            let total = self.daily_sale_totals[day_slot];
            if total <= 0 {
                continue;
            }
            // We can only finalize a daily bucket if the slot represents a
            // past date. We reconstruct the date from the slot index: find
            // dates in the sales VecDeque that map to this slot and are before
            // current_date.
            if let Some(date) = self.find_date_for_daily_slot(day_slot, current_date) {
                self.finalized_daily_mask |= bit;
                self.model
                    .observe_daily_total(date.weekday(), total as f64, as_of);
            }
        }

        // Finalize hourly buckets
        let current_hour = as_of.hour();
        for slot in 0..HOURLY_SLOTS {
            let byte_idx = slot / 8;
            let bit_idx = slot % 8;
            let bit = 1u8 << bit_idx;
            if self.finalized_hourly_mask[byte_idx] & bit != 0 {
                continue;
            }
            let total = self.hourly_sale_totals[slot];
            if total <= 0 {
                continue;
            }
            let slot_hour = (slot % 24) as u32;
            let slot_day_offset = slot / 24;
            if let Some(date) = self.find_date_for_daily_slot(slot_day_offset, current_date) {
                let is_past =
                    date < current_date || (date == current_date && slot_hour < current_hour);
                if is_past {
                    self.finalized_hourly_mask[byte_idx] |= bit;
                    self.model
                        .observe_hourly_total(slot_hour, total as f64, as_of);
                }
            }
        }
    }

    /// Find the actual NaiveDate that maps to a given daily slot index,
    /// by scanning the sales VecDeque for an entry whose date has that
    /// slot index and is before `before_date`.
    fn find_date_for_daily_slot(
        &self,
        day_slot: usize,
        before_date: NaiveDate,
    ) -> Option<NaiveDate> {
        self.sales
            .iter()
            .map(|entry| entry.at.date_naive())
            .find(|date| {
                date.num_days_from_ce() as usize % DAILY_SLOTS == day_slot && *date < before_date
            })
    }
}

fn insert_event_time_ordered(entries: &mut VecDeque<TimedUnits>, value: TimedUnits) {
    let index = entries
        .iter()
        .position(|entry| entry.at > value.at)
        .unwrap_or(entries.len());
    entries.insert(index, value);
}

fn sum_since(entries: &VecDeque<TimedUnits>, start: DateTime<Utc>, end: DateTime<Utc>) -> i32 {
    entries
        .iter()
        .filter(|entry| entry.at >= start && entry.at <= end)
        .map(|entry| entry.quantity)
        .sum()
}

#[derive(Debug, Default)]
pub struct FeatureEngine {
    states: HashMap<SkuLocation, SkuLocationState>,
}

impl FeatureEngine {
    pub fn state_mut(&mut self, key: SkuLocation) -> &mut SkuLocationState {
        self.states.entry(key).or_default()
    }

    pub fn states(&self) -> &HashMap<SkuLocation, SkuLocationState> {
        &self.states
    }

    pub fn states_mut(&mut self) -> &mut HashMap<SkuLocation, SkuLocationState> {
        &mut self.states
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Weekday};
    use data_platform_common::InventoryMovementType;

    use super::*;

    fn at(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, day, hour, minute, 0).unwrap()
    }

    fn balance(version: i64) -> InventoryBalanceState {
        InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: 100,
            reserved: 0,
            available: 100,
            safety_stock: 10,
            reorder_point: 20,
            max_stock: 200,
            version,
            updated_at: at(8, 0, 0),
        }
    }

    fn movement(
        id: Uuid,
        kind: InventoryMovementType,
        quantity: i32,
        when: DateTime<Utc>,
    ) -> InventoryMovementFact {
        InventoryMovementFact {
            movement_id: id,
            source_event_id: Uuid::new_v4(),
            sku_id: 42,
            location_code: "NCR".to_owned(),
            order_id: Some(1),
            movement_type: kind,
            quantity,
            occurred_at: when,
            recorded_at: when,
            resulting_balance_version: 2,
            reason: None,
        }
    }

    #[test]
    fn deduplicates_movements_and_rejects_stale_balances() {
        let mut state = SkuLocationState::default();
        assert!(state.apply_balance(balance(3)));
        assert!(!state.apply_balance(balance(3)));
        assert!(!state.apply_balance(balance(2)));

        let fact = movement(Uuid::new_v4(), InventoryMovementType::Sale, 2, at(8, 10, 0));
        assert!(state.apply_movement(&fact));
        assert!(!state.apply_movement(&fact));
    }

    #[test]
    fn snapshot_balance_can_reset_recovered_local_demo_state() {
        let mut state = SkuLocationState::default();
        assert!(state.apply_balance(balance(47)));
        assert!(!state.apply_balance(balance(1)));

        state.apply_balance_snapshot(balance(1));

        assert_eq!(state.balance.unwrap().version, 1);
    }

    #[test]
    fn uses_event_time_for_windows_and_does_not_count_reserve_as_sale() {
        let mut state = SkuLocationState::default();
        state.apply_balance(balance(1));
        state.apply_movement(&movement(
            Uuid::new_v4(),
            InventoryMovementType::Sale,
            3,
            at(8, 9, 58),
        ));
        state.apply_movement(&movement(
            Uuid::new_v4(),
            InventoryMovementType::Sale,
            5,
            at(8, 9, 40),
        ));
        state.apply_movement(&movement(
            Uuid::new_v4(),
            InventoryMovementType::Reserve,
            7,
            at(8, 9, 59),
        ));

        let features = state.calculate(at(8, 10, 0)).unwrap();
        assert_eq!(features.sale_units_5m, 3);
        assert_eq!(features.sale_units_15m, 3);
        assert_eq!(features.sale_units_1h, 8);
        assert_eq!(features.reserve_units_5m, 7);
    }

    #[test]
    fn sunday_forecast_exceeds_normal_tuesday_and_tuesday_spike_adapts() {
        let mut model = OnlineDemandModel::default();
        let training_time = at(1, 12, 0);
        for sample in [38.0, 40.0, 42.0, 39.0, 41.0, 40.0, 39.0, 41.0] {
            model.observe_daily_total(Weekday::Sun, sample, training_time);
        }
        for sample in [4.0, 5.0, 6.0, 5.0, 4.0, 6.0, 5.0, 5.0] {
            model.observe_daily_total(Weekday::Tue, sample, training_time);
        }

        let sunday = Utc.with_ymd_and_hms(2026, 9, 13, 12, 0, 0).unwrap();
        let tuesday = Utc.with_ymd_and_hms(2026, 9, 15, 12, 0, 0).unwrap();
        let sunday_forecast = model.forecast(sunday, 0.0, 0.0, 0.0);
        let normal_tuesday = model.forecast(tuesday, 0.0, 0.0, 0.0);
        let spike_tuesday = model.forecast(tuesday, 288.0, 192.0, 96.0);

        assert!(
            sunday_forecast.forecast_units_per_day > normal_tuesday.forecast_units_per_day * 5.0
        );
        assert!(spike_tuesday.spike_detected);
        assert!(
            spike_tuesday.forecast_units_per_day > normal_tuesday.forecast_units_per_day * 10.0
        );
    }

    #[test]
    fn forecast_horizon_keeps_weekday_shape_day_by_day() {
        let mut model = OnlineDemandModel::default();
        for sample in [80.0, 82.0, 78.0, 81.0] {
            model.observe_daily_total(Weekday::Sun, sample, at(1, 0, 0));
        }
        for sample in [10.0, 12.0, 11.0, 9.0] {
            model.observe_daily_total(Weekday::Fri, sample, at(1, 0, 0));
        }

        let friday = Utc.with_ymd_and_hms(2026, 9, 11, 10, 0, 0).unwrap();
        let horizon = model.forecast_horizon(friday, 3, 0.0, 0.0, 0.0);

        assert_eq!(
            horizon
                .iter()
                .map(|point| point.weekday.as_str())
                .collect::<Vec<_>>(),
            vec!["FRI", "SAT", "SUN"]
        );
        assert!(horizon[2].forecast_units > horizon[0].forecast_units * 5.0);
    }

    #[test]
    fn serialized_model_state_recovers_without_forgetting_learning() {
        let mut model = OnlineDemandModel::default();
        model.observe_daily_total(Weekday::Sun, 40.0, at(1, 0, 0));
        model.observe_recent_rate(25.0, at(2, 0, 0));
        let json = serde_json::to_string(&model).unwrap();
        let recovered: OnlineDemandModel = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, model);
    }

    #[test]
    fn calculate_does_not_relearn_the_same_finalized_buckets() {
        let mut state = SkuLocationState::default();
        state.apply_balance(balance(1));
        state.apply_movement(&movement(
            Uuid::new_v4(),
            InventoryMovementType::Sale,
            8,
            at(8, 23, 30),
        ));

        let first = state.calculate(at(9, 1, 0)).unwrap();
        let observations_after_first = state.model.observation_count;
        let second = state.calculate(at(9, 1, 30)).unwrap();

        assert_eq!(first.daily_sales, 0);
        assert_eq!(second.daily_sales, 0);
        assert_eq!(state.model.observation_count, observations_after_first);
    }

    #[test]
    fn hard_cap_limits_seen_movements_during_burst() {
        let mut state = SkuLocationState::default();
        let balance = InventoryBalanceState {
            sku_id: 1,
            location_code: "BOM".to_owned(),
            on_hand: 5000,
            reserved: 0,
            available: 5000,
            safety_stock: 100,
            reorder_point: 200,
            max_stock: 10000,
            version: 1,
            updated_at: Utc::now(),
        };
        state.apply_balance(balance);

        let now = Utc::now();
        // Send 700 movements within the 10-minute window
        for i in 0..700 {
            let fact = InventoryMovementFact {
                movement_id: Uuid::new_v4(),
                source_event_id: Uuid::new_v4(),
                sku_id: 1,
                location_code: "BOM".to_owned(),
                order_id: Some(i),
                movement_type: InventoryMovementType::Sale,
                quantity: 1,
                occurred_at: now + Duration::seconds(i as i64),
                recorded_at: now,
                resulting_balance_version: (i + 2) as i64,
                reason: None,
            };
            assert!(state.apply_movement(&fact));
        }

        // Must be capped at exactly 500 entries, preventing unbounded heap growth
        assert_eq!(state.seen_movements.len(), 500);
    }
}
