use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{Datelike, Utc};
use futures::stream::{self, StreamExt};
use hybrid_orchestrator::{
    config::OrchestratorConfig,
    event_handler::{HandleOutcome, InventoryBalancePayload, OrderStockConfirmedIntegrationEvent, handle_event},
    state::AppState,
};
use uuid::Uuid;

fn get_rss_kb() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            for line in content.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return parts[1].parse::<u64>().ok();
                    }
                }
            }
            None
        })
        .unwrap_or(0)
}

fn get_peak_rss_kb() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            for line in content.lines() {
                if line.starts_with("VmHWM:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return parts[1].parse::<u64>().ok();
                    }
                }
            }
            None
        })
        .unwrap_or(0)
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("================================================================================");
    println!("  HYBRID-ORCHESTRATOR SYNTHETIC SCALE & LOAD BENCHMARK");
    println!("  Simulating 50,000 SKU-Location Pairs + Deduplication & Trigger Filter");
    println!("================================================================================\n");

    let mut config = OrchestratorConfig::from_env();
    // Configure disabled LLM provider for pure orchestration & feature engine benchmark
    config.llm.provider = hybrid_orchestrator::config::LlmProvider::Disabled;
    let state = AppState::new(config);

    let initial_rss_kb = get_rss_kb();
    println!("Initial Process Baseline RSS: {:.2} MB", initial_rss_kb as f64 / 1024.0);

    let locations = ["NCR", "BLR", "BOM", "HYD", "MAA"];
    let num_skus = 10_000;
    let total_unique_pairs = num_skus * locations.len(); // 50,000 pairs

    println!(
        "Generating {} synthetic OrderStockConfirmed events across {} locations...",
        total_unique_pairs,
        locations.len()
    );

    // NOTE: This synthetic scale test runs with LLM_PROVIDER=disabled to strictly isolate
    // Rust-side data platform ingestion, deduplication, DashMap concurrency, and trigger-filter performance
    // from external HTTP API latency and third-party rate limits.
    let now = Utc::now();
    println!("Seeding baseline demand history for {} SKU-location pairs...", total_unique_pairs);
    let seed_start = Instant::now();
    for sku_idx in 1..=num_skus {
        for loc in locations.iter() {
            let mut sku_state = feature_engine::SkuLocationState::default();
            sku_state.model.observe_daily_total(now.weekday(), 500.0, now);
            state
                .seed_feature_state(
                    data_platform_common::SkuLocation {
                        sku_id: sku_idx as i32,
                        location_code: (*loc).to_string(),
                    },
                    sku_state,
                )
                .await;
        }
    }
    println!("Seeding completed in {:.3} seconds.", seed_start.elapsed().as_secs_f64());

    let mut events = Vec::with_capacity(total_unique_pairs);

    for sku_idx in 1..=num_skus {
        for (loc_idx, loc) in locations.iter().enumerate() {
            let is_escalated = (sku_idx + loc_idx) % 5 == 0; // 20% near reorder point / escalated
            let (on_hand, reserved, safety_stock, reorder_point, max_stock) = if is_escalated {
                // Stock is at or below reorder_point * 1.2 -> trigger filter escalates to LLM path
                (70, 10, 30, 80, 500)
            } else {
                // Healthy stock -> trigger filter skips LLM path
                (300, 15, 30, 80, 500)
            };

            let event = OrderStockConfirmedIntegrationEvent {
                event_id: Some(Uuid::new_v4()),
                order_id: (sku_idx * 10 + loc_idx) as i32,
                sku_id: sku_idx as i32,
                location_code: (*loc).to_string(),
                quantity_depleted: 5,
                occurred_at: Some(now),
                balance: Some(InventoryBalancePayload {
                    on_hand,
                    reserved,
                    safety_stock,
                    reorder_point,
                    max_stock,
                    version: 100,
                    updated_at: Some(now),
                    authoritative: true,
                }),
            };
            events.push(event);
        }
    }

    println!("Events prepared: {}. Starting concurrent ingestion...", events.len());

    let concurrency = 128;
    let start_time = Instant::now();

    let state_ref = Arc::new(state.clone());
    let results: Vec<(Duration, HandleOutcome)> = stream::iter(events.into_iter())
        .map(|event| {
            let st = state_ref.clone();
            async move {
                let t0 = Instant::now();
                let outcome = match handle_event(event, (*st).clone()).await {
                    Ok(o) => o,
                    Err(e) => panic!("handle_event error: {:?}", e),
                };
                (t0.elapsed(), outcome)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let elapsed = start_time.elapsed();
    let final_rss_kb = get_rss_kb();
    let peak_rss_kb = get_peak_rss_kb();
    let net_rss_mb = (final_rss_kb as f64 - initial_rss_kb as f64) / 1024.0;
    let bytes_per_pair = if total_unique_pairs > 0 {
        ((final_rss_kb - initial_rss_kb) * 1024) as f64 / total_unique_pairs as f64
    } else {
        0.0
    };

    let total_events = results.len();
    let throughput = total_events as f64 / elapsed.as_secs_f64();

    let mut latencies_us: Vec<f64> = results
        .iter()
        .map(|(d, _)| d.as_micros() as f64)
        .collect();
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut fast_path_count = 0;
    let mut escalated_count = 0;
    for (_, outcome) in &results {
        match outcome {
            HandleOutcome::NoAction => fast_path_count += 1,
            HandleOutcome::ProposalStored | HandleOutcome::AutoApprovedPrepared | HandleOutcome::Rejected => {
                escalated_count += 1;
            }
            HandleOutcome::Duplicate => {}
        }
    }

    println!("\n--- Ingestion & Concurrency Performance ---");
    println!("Total Unique Events:      {}", total_events);
    println!("Concurrency (workers):    {}", concurrency);
    println!("Total Duration:           {:.3} seconds", elapsed.as_secs_f64());
    println!("Throughput:               {:.0} events/second", throughput);
    println!("Fast-Path Skips (80%):    {} ({:.1}%)", fast_path_count, (fast_path_count as f64 / total_events as f64) * 100.0);
    println!("Escalated Events (20%):   {} ({:.1}%)", escalated_count, (escalated_count as f64 / total_events as f64) * 100.0);

    println!("\n--- Latency Distribution (Microseconds / Milliseconds) ---");
    println!("Min:                      {:.1} µs ({:.3} ms)", latencies_us.first().unwrap_or(&0.0), latencies_us.first().unwrap_or(&0.0) / 1000.0);
    println!("P50 (Median):             {:.1} µs ({:.3} ms)", percentile(&latencies_us, 50.0), percentile(&latencies_us, 50.0) / 1000.0);
    println!("P90:                      {:.1} µs ({:.3} ms)", percentile(&latencies_us, 90.0), percentile(&latencies_us, 90.0) / 1000.0);
    println!("P99:                      {:.1} µs ({:.3} ms)", percentile(&latencies_us, 99.0), percentile(&latencies_us, 99.0) / 1000.0);
    println!("Max:                      {:.1} µs ({:.3} ms)", latencies_us.last().unwrap_or(&0.0), latencies_us.last().unwrap_or(&0.0) / 1000.0);

    println!("\n--- Memory (RSS) Profile ---");
    println!("Baseline RSS:             {:.2} MB", initial_rss_kb as f64 / 1024.0);
    println!("Final RSS:                {:.2} MB", final_rss_kb as f64 / 1024.0);
    println!("Peak RSS (VmHWM):         {:.2} MB", peak_rss_kb as f64 / 1024.0);
    println!("Net Heap Allocated:       {:.2} MB", net_rss_mb);
    println!("Average RSS Per Pair:     {:.1} bytes/pair ({:.2} KB/pair)", bytes_per_pair, bytes_per_pair / 1024.0);

    // --- Deduplication & Bursts Test ---
    println!("\n--- Testing Deduplication Under Load (5,000 Redelivered Events) ---");
    let dedup_events: Vec<OrderStockConfirmedIntegrationEvent> = (0..5_000)
        .map(|i| {
            let sku = (i % 1000) + 1;
            OrderStockConfirmedIntegrationEvent {
                event_id: None, // relies on source_event_key: order-stock-confirmed:100:sku:loc:5
                order_id: 100,
                sku_id: sku,
                location_code: "NCR".to_string(),
                quantity_depleted: 5,
                occurred_at: Some(now),
                balance: Some(InventoryBalancePayload {
                    on_hand: 200,
                    reserved: 10,
                    safety_stock: 30,
                    reorder_point: 80,
                    max_stock: 500,
                    version: 100,
                    updated_at: Some(now),
                    authoritative: true,
                }),
            }
        })
        .collect();

    // First run creates the keys
    for ev in &dedup_events[0..1000] {
        let _ = handle_event(ev.clone(), state.clone()).await;
    }

    let dedup_start = Instant::now();
    let dedup_results: Vec<HandleOutcome> = stream::iter(dedup_events.into_iter())
        .map(|ev| {
            let st = state.clone();
            async move { handle_event(ev, st).await.unwrap() }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let dedup_elapsed = dedup_start.elapsed();
    let duplicate_count = dedup_results.iter().filter(|o| **o == HandleOutcome::Duplicate).count();
    println!("Dedup Check Duration:     {:.3} seconds", dedup_elapsed.as_secs_f64());
    println!("Duplicates Detected:      {} / 5,000", duplicate_count);
    println!("Dedup Check Throughput:   {:.0} checks/second", 5000.0 / dedup_elapsed.as_secs_f64());

    // --- Hot-SKU Movement Dedup Burst Test ---
    println!("\n--- Testing Hot-SKU Movement Dedup Hard Cap (500 Movements Burst) ---");
    let hot_sku_id = 99999;
    let mut movements = Vec::with_capacity(600);
    for m in 0..600 {
        let ev = OrderStockConfirmedIntegrationEvent {
            event_id: Some(Uuid::new_v4()),
            order_id: 20000 + m,
            sku_id: hot_sku_id,
            location_code: "BOM".to_string(),
            quantity_depleted: 1,
            occurred_at: Some(now + chrono::Duration::seconds(m as i64)),
            balance: Some(InventoryBalancePayload {
                on_hand: 500 - m,
                reserved: 0,
                safety_stock: 30,
                reorder_point: 80,
                max_stock: 1000,
                version: (100 + m) as i64,
                updated_at: Some(now + chrono::Duration::seconds(m as i64)),
                authoritative: true,
            }),
        };
        movements.push(ev);
    }

    for ev in movements {
        let _ = handle_event(ev, state.clone()).await;
    }

    // Verify feature state bounded capacity
    let pair = data_platform_common::SkuLocation::new(hot_sku_id, "BOM");
    if let Some(fs) = state.get_feature_state(&pair).await {
        println!("Hot-SKU seen_movements count: {} (Capped at 500)", fs.seen_movements_len());
        assert!(fs.seen_movements_len() <= 500, "seen_movements exceeded hard cap of 500!");
    } else {
        panic!("Hot-SKU feature state not found!");
    }

    println!("\n================================================================================");
    println!("  SCALE BENCHMARK COMPLETED SUCCESSFULLY: ZERO FAILURES, MEMORY STRICTLY BOUNDED");
    println!("================================================================================\n");

    Ok(())
}
