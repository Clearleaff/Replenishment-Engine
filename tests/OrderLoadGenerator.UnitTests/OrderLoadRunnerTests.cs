using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using eShop.OrderLoadGenerator;
using eShop.OrderLoadGenerator.Generation;
using eShop.OrderLoadGenerator.Ordering;
using eShop.OrderLoadGenerator.Telemetry;

namespace eShop.OrderLoadGenerator.UnitTests;

[TestClass]
public sealed class OrderLoadRunnerTests
{
    [TestMethod]
    public void Arrival_schedule_uses_open_loop_offsets()
    {
        Assert.AreEqual(600, ArrivalSchedule.GetPlannedArrivalCount(10, 60));
        Assert.AreEqual(TimeSpan.Zero, ArrivalSchedule.GetTargetOffset(0, 10));
        Assert.AreEqual(TimeSpan.FromMilliseconds(100), ArrivalSchedule.GetTargetOffset(1, 10));
        Assert.AreEqual(TimeSpan.FromSeconds(59.9), ArrivalSchedule.GetTargetOffset(599, 10));
    }

    [TestMethod]
    public async Task Slow_client_never_exceeds_bounded_concurrency_and_reports_drops()
    {
        var options = new OrderGeneratorOptions
        {
            RatePerSecond = 200,
            DurationSeconds = 0.1,
            MaxConcurrency = 2,
            DrainTimeoutSeconds = 2,
            RandomSeed = 42,
            CustomerPoolSize = 20
        };
        var statistics = new LoadRunStatistics();
        var runner = new OrderLoadRunner(
            new SlowOrderingClient(TimeSpan.FromMilliseconds(100)),
            Options.Create(options),
            statistics,
            NullLogger<OrderLoadRunner>.Instance);
        var products = Enumerable.Range(1, 10).Select(id => new CatalogProduct(id, $"P{id}", id)).ToArray();

        var snapshot = await runner.RunAsync(products, CancellationToken.None);

        Assert.IsLessThanOrEqualTo(options.MaxConcurrency, snapshot.MaxInFlight);
        Assert.IsGreaterThan(0, snapshot.Dropped);
        Assert.AreEqual(ArrivalSchedule.GetPlannedArrivalCount(options.RatePerSecond, options.DurationSeconds), snapshot.Offered);
        Assert.AreEqual(0, snapshot.CurrentInFlight);
    }

    [TestMethod]
    public void Variable_rate_wave_moves_above_and_below_base_rate()
    {
        var options = new OrderGeneratorOptions
        {
            RatePerSecond = 10,
            VariableRate = true,
            WaveAmplitudeFraction = 0.5,
            WavePeriodSeconds = 120,
            MinimumRatePerSecond = 5,
            MaximumRatePerSecond = 15
        };

        Assert.AreEqual(10, ArrivalSchedule.GetRateAtOffset(options, TimeSpan.Zero), 0.001);
        Assert.AreEqual(15, ArrivalSchedule.GetRateAtOffset(options, TimeSpan.FromSeconds(30)), 0.001);
        Assert.AreEqual(10, ArrivalSchedule.GetRateAtOffset(options, TimeSpan.FromSeconds(60)), 0.001);
        Assert.AreEqual(5, ArrivalSchedule.GetRateAtOffset(options, TimeSpan.FromSeconds(90)), 0.001);
    }

    [TestMethod]
    public void Fixed_rate_ignores_wave_configuration()
    {
        var options = new OrderGeneratorOptions
        {
            RatePerSecond = 10,
            VariableRate = false,
            WaveAmplitudeFraction = 0.9,
            WavePeriodSeconds = 120,
            MinimumRatePerSecond = 1,
            MaximumRatePerSecond = 20
        };

        Assert.AreEqual(10, ArrivalSchedule.GetRateAtOffset(options, TimeSpan.FromSeconds(30)), 0.001);
    }

    private sealed class SlowOrderingClient(TimeSpan delay) : IOrderingClient
    {
        public async Task<OrderSubmissionResult> SubmitAsync(OrderScenario scenario, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            return new(scenario.RequestId, OrderSubmissionClassification.Accepted, HttpStatusCode.OK, delay, 1);
        }
    }
}
