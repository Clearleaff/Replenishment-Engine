use std::collections::{HashMap, HashSet, VecDeque};

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
        self.hour[hour as usize].observe(units, self.decay_alpha);
        self.observation_count += 1;
        self.last_update = Some(at);
    }

    pub fn observe_recent_rate(&mut self, units_per_day: f64, at: DateTime<Utc>) {
        self.recent_rate_ewma = if self.recent_rate_ewma == 0.0 {
            units_per_day
        } else {
            0.35 * units_per_day + 0.65 * self.recent_rate_ewma
        };
        self.last_update = Some(at);
    }

    pub fn weekday_stat(&self, weekday: Weekday) -> &DecayedStat {
        &self.weekday[weekday.num_days_from_monday() as usize]
    }

    pub fn hour_stat(&self, hour: u32) -> &DecayedStat {
        &self.hour[hour as usize]
    }

    pub fn forecast(
        &self,
        at: DateTime<Utc>,
        velocity_5m: f64,
        velocity_15m: f64,
        velocity_1h: f64,
    ) -> AdaptiveForecast {
        let weekday = self.weekday_stat(at.weekday());
        let hour = self.hour_stat(at.hour());
        let baseline = match (weekday.observation_count, hour.observation_count) {
            (0, 0) => self.recent_rate_ewma.max(0.0),
            (_, 0) => weekday.mean,
            (0, _) => hour.mean * 24.0,
            _ => 0.75 * weekday.mean + 0.25 * hour.mean * 24.0,
        };
        let weighted_recent = 0.55 * velocity_5m + 0.30 * velocity_15m + 0.15 * velocity_1h;
        let recent = if weighted_recent > 0.0 {
            if self.recent_rate_ewma > 0.0 {
                0.75 * weighted_recent + 0.25 * self.recent_rate_ewma
            } else {
                weighted_recent
            }
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

#[derive(Debug, Clone, Default)]
pub struct SkuLocationState {
    pub balance: Option<InventoryBalanceState>,
    pub model: OnlineDemandModel,
    seen_movements: HashSet<Uuid>,
    sales: VecDeque<TimedUnits>,
    reserves: VecDeque<TimedUnits>,
    daily_sale_totals: HashMap<NaiveDate, i32>,
    hourly_sale_totals: HashMap<(NaiveDate, u32), i32>,
    finalized_daily_sale_dates: HashSet<NaiveDate>,
    finalized_hourly_sale_buckets: HashSet<(NaiveDate, u32)>,
    last_recent_rate_observation_at: Option<DateTime<Utc>>,
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

    pub fn apply_movement(&mut self, fact: &InventoryMovementFact) -> bool {
        if !self.seen_movements.insert(fact.movement_id) {
            return false;
        }
        let units = TimedUnits {
            at: fact.occurred_at,
            quantity: fact.quantity,
        };
        match fact.movement_type {
            InventoryMovementType::Sale => {
                insert_event_time_ordered(&mut self.sales, units);
                *self
                    .daily_sale_totals
                    .entry(fact.occurred_at.date_naive())
                    .or_default() += fact.quantity;
                *self
                    .hourly_sale_totals
                    .entry((fact.occurred_at.date_naive(), fact.occurred_at.hour()))
                    .or_default() += fact.quantity;
            }
            InventoryMovementType::Reserve => insert_event_time_ordered(&mut self.reserves, units),
            _ => {}
        }
        true
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
        for (date, total) in self.daily_sale_totals.clone() {
            if date >= current_date || total <= 0 || !self.finalized_daily_sale_dates.insert(date) {
                continue;
            }
            self.model
                .observe_daily_total(date.weekday(), total as f64, as_of);
        }

        let current_bucket = (current_date, as_of.hour());
        for (bucket, total) in self.hourly_sale_totals.clone() {
            if bucket >= current_bucket
                || total <= 0
                || !self.finalized_hourly_sale_buckets.insert(bucket)
            {
                continue;
            }
            self.model
                .observe_hourly_total(bucket.1, total as f64, as_of);
        }
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
}
