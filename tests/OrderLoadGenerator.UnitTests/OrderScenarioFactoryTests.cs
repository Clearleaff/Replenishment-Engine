using eShop.OrderLoadGenerator;
using eShop.OrderLoadGenerator.Generation;

namespace eShop.OrderLoadGenerator.UnitTests;

[TestClass]
public sealed class OrderScenarioFactoryTests
{
    private static readonly CatalogProduct[] Products = Enumerable.Range(1, 20)
        .Select(id => new CatalogProduct(id, $"Product {id}", id))
        .ToArray();

    [TestMethod]
    [DataRow("NORMAL", "Normal")]
    [DataRow("WEEKDAY_SEASONAL", "WeekdaySeasonal")]
    [DataRow("PROMOTION", "Promotion")]
    [DataRow("VIRAL", "Viral")]
    [DataRow("REGIONAL_SPIKE", "RegionalSpike")]
    public void Documented_profile_names_normalize_for_configuration_binding(string input, string expected)
    {
        Assert.AreEqual(expected, OrderWorkloadProfiles.Normalize(input));
    }

    [TestMethod]
    public void Fixed_seed_repeats_business_sequence_and_always_creates_valid_orders()
    {
        var options = CreateOptions();
        var first = new OrderScenarioFactory(Products, options);
        var second = new OrderScenarioFactory(Products, options);

        for (var index = 0; index < 1_000; index++)
        {
            var left = first.Create();
            var right = second.Create();

            Assert.AreEqual(left.CustomerId, right.CustomerId);
            Assert.AreEqual(left.LocationCode, right.LocationCode);
            CollectionAssert.AreEqual(
                left.Items.Select(item => (item.ProductId, item.Quantity)).ToArray(),
                right.Items.Select(item => (item.ProductId, item.Quantity)).ToArray());
            CollectionAssert.Contains(OrderScenarioFactory.SupportedLocations, left.LocationCode);
            Assert.IsGreaterThanOrEqualTo(1, left.Items.Count);
            Assert.IsLessThanOrEqualTo(4, left.Items.Count);
            Assert.AreEqual(left.Items.Count, left.Items.Select(item => item.ProductId).Distinct().Count());
            Assert.IsTrue(left.Items.All(item => item.Quantity is >= 1 and <= 3));
        }
    }

    [TestMethod]
    public void Weighted_location_and_hot_sku_distribution_are_visible_over_a_large_sample()
    {
        var factory = new OrderScenarioFactory(Products, CreateOptions());
        var locations = OrderScenarioFactory.SupportedLocations.ToDictionary(code => code, _ => 0);
        var hotLines = 0;
        var allLines = 0;

        for (var index = 0; index < 20_000; index++)
        {
            var scenario = factory.Create();
            locations[scenario.LocationCode]++;
            hotLines += scenario.Items.Count(item => item.ProductId <= 4);
            allLines += scenario.Items.Count;
        }

        Assert.IsTrue(locations["NCR"] is > 7_400 and < 8_600);
        Assert.IsTrue(locations["BLR"] is > 4_400 and < 5_600);
        Assert.IsTrue(locations["BOM"] is > 3_400 and < 4_600);
        Assert.IsTrue(locations["HYD"] is > 2_400 and < 3_600);
        Assert.IsGreaterThan(0.65, (double)hotLines / allLines);
    }

    [TestMethod]
    public void Logical_request_ids_are_unique()
    {
        var factory = new OrderScenarioFactory(Products, CreateOptions());
        var ids = Enumerable.Range(0, 5_000).Select(_ => factory.Create().RequestId).ToArray();

        Assert.AreEqual(ids.Length, ids.Distinct().Count());
        Assert.IsFalse(ids.Contains(Guid.Empty));
    }

    [TestMethod]
    public void Regional_spike_concentrates_sku_42_in_ncr_without_changing_normal_defaults()
    {
        var products = Enumerable.Range(1, 50)
            .Select(id => new CatalogProduct(id, $"Product {id}", id))
            .ToArray();
        var options = CreateOptions();
        options.Profile = OrderWorkloadProfile.RegionalSpike;
        var factory = new OrderScenarioFactory(products, options);

        var scenarios = Enumerable.Range(0, 2_000).Select(_ => factory.Create()).ToArray();

        Assert.IsTrue(scenarios.All(scenario => scenario.LocationCode == "NCR"));
        Assert.IsGreaterThan(0.85, scenarios.Count(scenario => scenario.Items.Any(item => item.ProductId == 42)) / 2_000d);
    }

    [TestMethod]
    public void Weekday_seasonal_profile_keeps_sunday_target_demand_above_tuesday()
    {
        var products = Enumerable.Range(1, 50)
            .Select(id => new CatalogProduct(id, $"Product {id}", id))
            .ToArray();
        var sundayOptions = CreateOptions();
        sundayOptions.Profile = OrderWorkloadProfile.WeekdaySeasonal;
        sundayOptions.SimulatedDayOfWeek = (int)DayOfWeek.Sunday;
        var tuesdayOptions = CreateOptions();
        tuesdayOptions.Profile = OrderWorkloadProfile.WeekdaySeasonal;
        tuesdayOptions.SimulatedDayOfWeek = (int)DayOfWeek.Tuesday;

        var sunday = new OrderScenarioFactory(products, sundayOptions);
        var tuesday = new OrderScenarioFactory(products, tuesdayOptions);
        var sundayHits = Enumerable.Range(0, 2_000).Count(_ => sunday.Create().Items.Any(item => item.ProductId == 42));
        var tuesdayHits = Enumerable.Range(0, 2_000).Count(_ => tuesday.Create().Items.Any(item => item.ProductId == 42));

        Assert.IsGreaterThan(tuesdayHits * 3, sundayHits);
    }

    private static OrderGeneratorOptions CreateOptions() => new()
    {
        RandomSeed = 42,
        CustomerPoolSize = 200,
        HotSkuFraction = 0.2,
        HotTrafficShare = 0.8
    };
}
