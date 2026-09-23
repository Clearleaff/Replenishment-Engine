-- ClickHouse Schema & Seed Data for Event Calendar and Supplier Disruption Signals
-- Database: eshop_analytics

CREATE TABLE IF NOT EXISTS eshop_analytics.event_calendar (
    event_id UUID,
    location_code LowCardinality(String),
    event_name String,
    event_type LowCardinality(String),
    start_date Date,
    end_date Date,
    expected_demand_impact LowCardinality(String),
    notes Nullable(String),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (location_code, start_date, event_id);

CREATE TABLE IF NOT EXISTS eshop_analytics.supplier_disruption_signals (
    signal_id UUID,
    sku_id Nullable(Int32),
    location_code LowCardinality(String),
    signal_type LowCardinality(String),
    severity LowCardinality(String),
    description String,
    reported_at DateTime64(6, 'UTC'),
    resolved_at Nullable(DateTime64(6, 'UTC')),
    source LowCardinality(String),
    created_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (location_code, reported_at, signal_id);

-- Seed event_calendar with Indian festivals, national events, and e-commerce sales for 2026-2027
-- Across major warehouse hubs: ALL (general), NCR, BLR, BOM, HYD

INSERT INTO eshop_analytics.event_calendar VALUES
('11111111-1111-1111-1111-111111110001', 'ALL', 'Great Indian Festival / Big Billion Days Sale', 'PROMOTION', '2026-10-08', '2026-10-16', 'HIGH', 'Annual flagship festive sale across all categories', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110002', 'ALL', 'Diwali Festive Week', 'FESTIVAL', '2026-11-06', '2026-11-11', 'HIGH', 'Diwali and Dhanteras gifting rush', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110003', 'ALL', 'Dussehra / Navratri Rush', 'FESTIVAL', '2026-10-18', '2026-10-21', 'MEDIUM', 'Navratri shopping and festive replenishment', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110004', 'BLR', 'Bangalore Tech Summit & Cyber Week Sale', 'PROMOTION', '2026-11-20', '2026-11-25', 'MEDIUM', 'High electronic and merchandise demand in Bangalore tech corridor', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110005', 'BOM', 'Ganesh Chaturthi Festive Surge', 'FESTIVAL', '2026-09-14', '2026-09-24', 'HIGH', 'Peak regional holiday and festive apparel/sweets demand in Maharashtra', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110006', 'NCR', 'Pre-Winter Shopping Carnival', 'SEASONAL', '2026-11-01', '2026-11-15', 'MEDIUM', 'North India winter season catalog replenishment spike', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110007', 'ALL', 'Christmas & Year-End Clearance Sale', 'PROMOTION', '2026-12-20', '2026-12-31', 'HIGH', 'National holiday rush and inventory liquidation discounts', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110008', 'ALL', 'Republic Day Sale 2027', 'PROMOTION', '2027-01-20', '2027-01-26', 'HIGH', 'Republic Day mega electronics and apparel sale', now64(6), now64(6)),
('11111111-1111-1111-1111-111111110009', 'ALL', 'Holi Festival of Colors 2027', 'FESTIVAL', '2027-03-20', '2027-03-24', 'MEDIUM', 'Spring festival seasonal demand surge', now64(6), now64(6));

-- Seed supplier_disruption_signals with 4 synthetic test entries for verification purposes

INSERT INTO eshop_analytics.supplier_disruption_signals VALUES
('22222222-2222-2222-2222-222222220001', 2, 'NCR', 'DELAY', 'MEDIUM', 'Primary supplier freight delay on NH-48; delivery transit extended by +36 hours', '2026-09-22 06:00:00.000000', NULL, 'SUPPLIER_API', now64(6)),
('22222222-2222-2222-2222-222222220002', NULL, 'BOM', 'LOGISTICS', 'HIGH', 'Port congestion at JNPT affecting inbound container clearing for Western India', '2026-09-21 12:00:00.000000', NULL, 'MANUAL', now64(6)),
('22222222-2222-2222-2222-222222220003', 42, 'BLR', 'SHORTAGE', 'HIGH', 'Factory raw material shortage for SKU 42 batch; allocation constrained to 50%', '2026-09-20 09:30:00.000000', NULL, 'SUPPLIER_API', now64(6)),
('22222222-2222-2222-2222-222222220004', 1, 'NCR', 'QUALITY', 'LOW', 'Minor packaging batch defect reported and cleared after reinspection', '2026-09-18 14:00:00.000000', '2026-09-19 18:00:00.000000', 'AUTOMATED', now64(6));

