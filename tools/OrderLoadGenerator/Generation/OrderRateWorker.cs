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

    public static double GetRateAtOffset(OrderGeneratorOptions options, TimeSpan elapsed)
    {
        if (!options.VariableRate)
        {
            return options.RatePerSecond;
        }

        var period = Math.Max(1, options.WavePeriodSeconds);
        var radians = 2 * Math.PI * (elapsed.TotalSeconds % period) / period;
        var multiplier = 1 + options.WaveAmplitudeFraction * Math.Sin(radians);
        var unclamped = options.RatePerSecond * multiplier;
        return Math.Clamp(unclamped, options.MinimumRatePerSecond, options.MaximumRatePerSecond);
    }

    public static TimeSpan GetVariableArrivalDelay(OrderGeneratorOptions options, TimeSpan elapsed) =>
        TimeSpan.FromSeconds(1 / GetRateAtOffset(options, elapsed));
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

        statistics.Reset();
        var duration = TimeSpan.FromSeconds(settings.DurationSeconds);
        var stopwatch = Stopwatch.StartNew();
        var nextProgress = TimeSpan.FromSeconds(5);

        if (settings.VariableRate)
        {
            await OfferVariableRateAsync(settings, scenarioFactory, queue.Writer, stopwatch, duration, nextProgress, cancellationToken);
        }
        else
        {
            await OfferFixedRateAsync(settings, scenarioFactory, queue.Writer, stopwatch, nextProgress, cancellationToken);
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

    private async Task OfferFixedRateAsync(
        OrderGeneratorOptions settings,
        OrderScenarioFactory scenarioFactory,
        ChannelWriter<OrderScenario> writer,
        Stopwatch stopwatch,
        TimeSpan nextProgress,
        CancellationToken cancellationToken)
    {
        var totalArrivals = ArrivalSchedule.GetPlannedArrivalCount(settings.RatePerSecond, settings.DurationSeconds);
        for (var index = 0; index < totalArrivals; index++)
        {
            var target = ArrivalSchedule.GetTargetOffset(index, settings.RatePerSecond);
            var remaining = target - stopwatch.Elapsed;
            if (remaining > TimeSpan.Zero)
            {
                await Task.Delay(remaining, cancellationToken);
            }

            nextProgress = OfferOne(scenarioFactory, writer, stopwatch, nextProgress);
        }
    }

    private async Task OfferVariableRateAsync(
        OrderGeneratorOptions settings,
        OrderScenarioFactory scenarioFactory,
        ChannelWriter<OrderScenario> writer,
        Stopwatch stopwatch,
        TimeSpan duration,
        TimeSpan nextProgress,
        CancellationToken cancellationToken)
    {
        while (stopwatch.Elapsed < duration)
        {
            nextProgress = OfferOne(scenarioFactory, writer, stopwatch, nextProgress);
            var delay = ArrivalSchedule.GetVariableArrivalDelay(settings, stopwatch.Elapsed);
            if (stopwatch.Elapsed + delay > duration)
            {
                break;
            }

            await Task.Delay(delay, cancellationToken);
        }
    }

    private TimeSpan OfferOne(
        OrderScenarioFactory scenarioFactory,
        ChannelWriter<OrderScenario> writer,
        Stopwatch stopwatch,
        TimeSpan nextProgress)
    {
        var scenario = scenarioFactory.Create();
        statistics.RecordOffered(scenario);
        if (!writer.TryWrite(scenario))
        {
            statistics.RecordDropped();
        }

        if (stopwatch.Elapsed >= nextProgress)
        {
            var progress = statistics.CreateSnapshot(stopwatch.Elapsed, stopwatch.Elapsed);
            var currentRate = ArrivalSchedule.GetRateAtOffset(options.Value, stopwatch.Elapsed);
            logger.LogInformation(
                "Load progress: elapsed={Elapsed:F1}s currentTargetRate={CurrentRate:F2}/s offered={Offered} accepted={Accepted} dropped={Dropped} inFlight={InFlight}",
                stopwatch.Elapsed.TotalSeconds,
                currentRate,
                progress.Offered,
                progress.Accepted,
                progress.Dropped,
                progress.CurrentInFlight);
            nextProgress += TimeSpan.FromSeconds(5);
        }

        return nextProgress;
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
                "Preparing load: profile={Profile} baseRate={Rate:F2}/s ({OrdersPerMinute:F0}/min) duration={Duration:F1}s continuous={Continuous} variableRate={VariableRate} waveAmplitude={WaveAmplitude:P0} wavePeriod={WavePeriod:F0}s concurrency={Concurrency} seed={Seed}",
                settings.Profile,
                settings.RatePerSecond,
                settings.RatePerSecond * 60,
                settings.DurationSeconds,
                settings.Continuous,
                settings.VariableRate,
                settings.WaveAmplitudeFraction,
                settings.WavePeriodSeconds,
                settings.MaxConcurrency,
                settings.RandomSeed);

            var products = await catalog.GetProductsAsync(stoppingToken);
            await tokenProvider.GetAccessTokenAsync(stoppingToken);
            logger.LogInformation("Preflight complete: cached {SkuCount} Catalog SKUs and one access token", products.Count);

            var cycle = 0;
            do
            {
                cycle++;
                logger.LogInformation(
                    "Starting order load cycle {Cycle}: baseRate={Rate:F2}/s ({OrdersPerMinute:F0}/min)",
                    cycle,
                    settings.RatePerSecond,
                    settings.RatePerSecond * 60);

                var snapshot = await runner.RunAsync(products, stoppingToken);
                Console.WriteLine(LoadRunStatistics.FormatSummary(snapshot, settings, products.Count, cycle));

                if (snapshot.Dropped > 0 || snapshot.DuplicateRequestIds > 0 ||
                    snapshot.HttpFailures > 0 || snapshot.Timeouts > 0 || snapshot.CurrentInFlight > 0)
                {
                    Environment.ExitCode = 1;
                    logger.LogWarning(
                        "Order load cycle {Cycle} completed with load-generator errors; continuous mode will keep running unless cancelled",
                        cycle);
                }
            }
            while (settings.Continuous && !stoppingToken.IsCancellationRequested);
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
