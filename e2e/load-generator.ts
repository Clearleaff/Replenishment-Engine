import { test, request } from '@playwright/test';

test('Simulate Demand Spike (High Volatility)', async () => {
    // We create a new API context to hammer the Ordering API
    const apiContext = await request.newContext({
        // Targeting the eShop web API gateway or ordering API directly
        baseURL: process.env.API_URL || 'http://localhost:5102', 
        ignoreHTTPSErrors: true,
    });

    console.log("🚀 Starting Load Generator: Simulating high-volatility demand spike...");

    const SKU_ID = 42; // Example SKU ID 
    const LOCATION_CODE = "NCR";
    
    let successCount = 0;
    let failCount = 0;

    // Simulate 50 orders in a rapid burst
    for (let i = 0; i < 50; i++) {
        // eShop standard basket/order payload
        const payload = {
            city: "Redmond",
            street: "One Microsoft Way",
            state: "WA",
            country: "USA",
            zipCode: "98052",
            cardNumber: "1111222233334444",
            cardHolderName: "Load Tester",
            cardExpiration: "12/28",
            cardSecurityNumber: "123",
            cardTypeId: 1,
            buyer: "load-tester",
            items: [
                {
                    productId: SKU_ID,
                    productName: "Premium Item",
                    unitPrice: 15.0,
                    discount: 0,
                    units: 1, // 1 unit per order, 50 distinct orders
                    pictureUrl: "fake.png"
                }
            ]
        };

        const response = await apiContext.post('/api/v1/orders', {
            data: payload,
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok()) {
            successCount++;
        } else {
            failCount++;
        }

        // Wait 100ms between requests to spread it over 5 seconds (50 * 100ms = 5000ms)
        await new Promise(r => setTimeout(r, 100));
    }

    console.log(`✅ Load generation complete.`);
    console.log(`   Success: ${successCount}`);
    console.log(`   Failed:  ${failCount}`);
    console.log(`\nThe hybrid-orchestrator's SkuLocationState EWMA should now trip the spike_score threshold.`);
});
