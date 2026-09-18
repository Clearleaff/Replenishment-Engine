using System.Net;
using eShop.OrderLoadGenerator;
using eShop.OrderLoadGenerator.Generation;
using eShop.OrderLoadGenerator.Ordering;
using eShop.OrderLoadGenerator.Telemetry;

namespace eShop.OrderLoadGenerator.UnitTests;

[TestClass]
public sealed class LoadRunStatisticsTests
{
    [TestMethod]
    public void Snapshot_calculates_counts_rates_shapes_and_nearest_rank_percentiles()
    {
        var statistics = new LoadRunStatistics();
        var requestId = Guid.NewGuid();
        var scenario = new OrderScenario(requestId, "one", "One", "NCR",
        [
            new("1", 1, "One", 1, 1, 1, null),
            new("2", 2, "Two", 2, 2, 3, null)
        ]);
        statistics.RecordOffered(scenario);
        statistics.RecordOffered(scenario);

        foreach (var milliseconds in Enumerable.Range(1, 100))
        {
            statistics.RequestStarted();
            statistics.RequestCompleted(new(
                Guid.NewGuid(),
                OrderSubmissionClassification.Accepted,
                HttpStatusCode.OK,
                TimeSpan.FromMilliseconds(milliseconds),
                1));
        }

        var snapshot = statistics.CreateSnapshot(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));

        Assert.AreEqual(2, snapshot.Offered);
        Assert.AreEqual(1, snapshot.OfferedPerSecond);
        Assert.AreEqual(1, snapshot.DuplicateRequestIds);
        Assert.AreEqual(2, snapshot.AverageLinesPerOrder);
        Assert.AreEqual(4, snapshot.AverageUnitsPerOrder);
        Assert.AreEqual(50, snapshot.P50Milliseconds);
        Assert.AreEqual(95, snapshot.P95Milliseconds);
        Assert.AreEqual(99, snapshot.P99Milliseconds);
        Assert.AreEqual(100, snapshot.StatusCodes[200]);
    }

    [TestMethod]
    public void Empty_percentile_is_zero()
    {
        Assert.AreEqual(0, LoadRunStatistics.Percentile([], 0.95));
    }
}
