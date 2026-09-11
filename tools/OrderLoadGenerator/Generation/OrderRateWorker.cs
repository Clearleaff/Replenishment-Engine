using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using eShop.OrderLoadGenerator.Authentication;
using eShop.OrderLoadGenerator.Catalog;
using eShop.OrderLoadGenerator.Ordering;
using eShop.OrderLoadGenerator.Telemetry;

namespace eShop.OrderLoadGenerator.Generation;

public static class ArrivalSchedule
{
    public static int GetPlannedArrivalCount(double ratePerSecond, double durationSeconds) =>
        checked((int)Math.Floor(ratePerSecond * durationSeconds));

    public static TimeSpan GetTargetOffset(int zeroBasedArrival, double ratePerSecond) =>
        TimeSpan.FromSeconds(zeroBasedArrival / ratePerSecond);
}

public sealed class OrderLoadRunner(
    IOrderingClient orderingClient,
    IOptions<OrderGeneratorOptions> options,
    LoadRunStatistics statistics,
    ILogger<OrderLoadRunner> logger)
{
    public async Task<LoadRunSnapshot> RunAsync(
        IReadOnlyList<CatalogProduct> products,
        CancellationToken cancellationToken)
    {
        var settings = options.Value;
        var scenarioFactory = new OrderScenarioFactory(products, settings);
        var queue = Channel.CreateBounded<OrderScenario>(new BoundedChannelOptions(settings.MaxConcurrency)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        using var workerCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var consumers = Enumerable.Range(0, settings.MaxConcurrency)
            .Select(_ => ConsumeAsync(queue.Reader, workerCancellation.Token))
            .ToArray();

        var totalArrivals = ArrivalSchedule.GetPlannedArrivalCount(settings.RatePerSecond, settings.DurationSeconds);
        var duration = TimeSpan.FromSeconds(settings.DurationSeconds);
        var stopwatch = Stopwatch.StartNew();
        var nextProgress = TimeSpan.FromSeconds(5);

        for (var index = 0; index < totalArrivals; index++)
        {
            var target = ArrivalSchedule.GetTargetOffset(index, settings.RatePerSecond);
            var remaining = target - stopwatch.Elapsed;
            if (remaining > TimeSpan.Zero)
            {
                await Task.Delay(remaining, cancellationToken);
            }

            var scenario = scenarioFactory.Create();
            statistics.RecordOffered(scenario);
            if (!queue.Writer.TryWrite(scenario))
            {
                statistics.RecordDropped();
            }

            if (stopwatch.Elapsed >= nextProgress)
            {
                var progress = statistics.CreateSnapshot(stopwatch.Elapsed, stopwatch.Elapsed);
                logger.LogInformation(
                    "Load progress: elapsed={Elapsed:F1}s offered={Offered} accepted={Accepted} dropped={Dropped} inFlight={InFlight}",
                    stopwatch.Elapsed.TotalSeconds,
                    progress.Offered,
                    progress.Accepted,
                    progress.Dropped,
                    progress.CurrentInFlight);
                nextProgress += TimeSpan.FromSeconds(5);
            }
        }

        var untilWindowEnd = duration - stopwatch.Elapsed;
        if (untilWindowEnd > TimeSpan.Zero)
        {
            await Task.Delay(untilWindowEnd, cancellationToken);
        }

        queue.Writer.Complete();
        var offerWindow = stopwatch.Elapsed;
        try
        {
            await Task.WhenAll(consumers).WaitAsync(TimeSpan.FromSeconds(settings.DrainTimeoutSeconds), cancellationToken);
        }
        catch (TimeoutException)
        {
            logger.LogError("In-flight requests did not drain within {DrainTimeoutSeconds} seconds", settings.DrainTimeoutSeconds);
            workerCancellation.Cancel();
            await IgnoreCancellationAsync(consumers);
        }

        return statistics.CreateSnapshot(offerWindow, stopwatch.Elapsed);
    }

    private async Task ConsumeAsync(ChannelReader<OrderScenario> reader, CancellationToken cancellationToken)
    {
        await foreach (var scenario in reader.ReadAllAsync(cancellationToken))
        {
            statistics.RequestStarted();
            try
            {
                var result = await orderingClient.SubmitAsync(scenario, cancellationToken);
                statistics.RequestCompleted(result);
            }
            catch (OperationCanceledException)
            {
                statistics.RequestCompleted(new(
                    scenario.RequestId,
                    OrderSubmissionClassification.Timeout,
                    null,
                    TimeSpan.Zero,
                    0));
            }
        }
    }

    private static async Task IgnoreCancellationAsync(Task[] tasks)
    {
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
        }
    }
}

public sealed class OrderRateWorker(
    ICatalogSnapshotProvider catalog,
    IAccessTokenProvider tokenProvider,
    OrderLoadRunner runner,
    IOptions<OrderGeneratorOptions> options,
    IHostApplicationLifetime lifetime,
    ILogger<OrderRateWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var settings = options.Value;
            logger.LogInformation(
                "Preparing load: profile={Profile} rate={Rate:F2}/s duration={Duration:F1}s concurrency={Concurrency} seed={Seed}",
                settings.Profile,
                settings.RatePerSecond,
                settings.DurationSeconds,
                settings.MaxConcurrency,
                settings.RandomSeed);

            var products = await catalog.GetProductsAsync(stoppingToken);
            await tokenProvider.GetAccessTokenAsync(stoppingToken);
            logger.LogInformation("Preflight complete: cached {SkuCount} Catalog SKUs and one access token", products.Count);

            var snapshot = await runner.RunAsync(products, stoppingToken);
            Console.WriteLine(LoadRunStatistics.FormatSummary(snapshot, settings, products.Count));

            if (snapshot.Dropped > 0 || snapshot.DuplicateRequestIds > 0 ||
                snapshot.HttpFailures > 0 || snapshot.Timeouts > 0 || snapshot.CurrentInFlight > 0)
            {
                Environment.ExitCode = 1;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning("Order load run was cancelled");
            Environment.ExitCode = 2;
        }
        catch (Exception exception)
        {
            logger.LogCritical(exception, "Order load run failed before producing a valid summary");
            Environment.ExitCode = 1;
        }
        finally
        {
            lifetime.StopApplication();
        }
    }
}
