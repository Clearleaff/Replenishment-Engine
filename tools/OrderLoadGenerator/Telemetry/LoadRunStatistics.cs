using System.Collections.Concurrent;
using System.Net;
using System.Text;
using eShop.OrderLoadGenerator.Generation;
using eShop.OrderLoadGenerator.Ordering;

namespace eShop.OrderLoadGenerator.Telemetry;

public sealed class LoadRunStatistics
{
    private readonly ConcurrentDictionary<Guid, byte> _requestIds = new();
    private readonly ConcurrentDictionary<int, long> _statusCodes = new();
    private readonly ConcurrentDictionary<string, long> _locations = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<double> _latenciesMilliseconds = new();
    private long _offered;
    private long _accepted;
    private long _httpFailures;
    private long _timeouts;
    private long _dropped;
    private long _duplicates;
    private long _totalLines;
    private long _totalUnits;
    private int _inFlight;
    private int _maxInFlight;

    public void Reset()
    {
        _requestIds.Clear();
        _statusCodes.Clear();
        _locations.Clear();
        _latenciesMilliseconds.Clear();
        Interlocked.Exchange(ref _offered, 0);
        Interlocked.Exchange(ref _accepted, 0);
        Interlocked.Exchange(ref _httpFailures, 0);
        Interlocked.Exchange(ref _timeouts, 0);
        Interlocked.Exchange(ref _dropped, 0);
        Interlocked.Exchange(ref _duplicates, 0);
        Interlocked.Exchange(ref _totalLines, 0);
        Interlocked.Exchange(ref _totalUnits, 0);
        Interlocked.Exchange(ref _inFlight, 0);
        Interlocked.Exchange(ref _maxInFlight, 0);
    }

    public void RecordOffered(OrderScenario scenario)
    {
        Interlocked.Increment(ref _offered);
        Interlocked.Add(ref _totalLines, scenario.Items.Count);
        Interlocked.Add(ref _totalUnits, scenario.TotalUnits);
        _locations.AddOrUpdate(scenario.LocationCode, 1, static (_, count) => count + 1);
        if (!_requestIds.TryAdd(scenario.RequestId, 0))
        {
            Interlocked.Increment(ref _duplicates);
        }
    }

    public void RecordDropped() => Interlocked.Increment(ref _dropped);

    public void RequestStarted()
    {
        var current = Interlocked.Increment(ref _inFlight);
        var observed = Volatile.Read(ref _maxInFlight);
        while (current > observed)
        {
            observed = Interlocked.CompareExchange(ref _maxInFlight, current, observed);
        }
    }

    public void RequestCompleted(OrderSubmissionResult result)
    {
        Interlocked.Decrement(ref _inFlight);
        _latenciesMilliseconds.Enqueue(result.Latency.TotalMilliseconds);
        if (result.StatusCode is { } statusCode)
        {
            _statusCodes.AddOrUpdate((int)statusCode, 1, static (_, count) => count + 1);
        }

        switch (result.Classification)
        {
            case OrderSubmissionClassification.Accepted:
                Interlocked.Increment(ref _accepted);
                break;
            case OrderSubmissionClassification.Timeout:
                Interlocked.Increment(ref _timeouts);
                break;
            default:
                Interlocked.Increment(ref _httpFailures);
                break;
        }
    }

    public LoadRunSnapshot CreateSnapshot(TimeSpan offerWindow, TimeSpan totalElapsed)
    {
        var latencies = _latenciesMilliseconds.Order().ToArray();
        var offered = Volatile.Read(ref _offered);
        return new(
            offerWindow,
            totalElapsed,
            offered,
            offerWindow.TotalSeconds > 0 ? offered / offerWindow.TotalSeconds : 0,
            Volatile.Read(ref _accepted),
            Volatile.Read(ref _httpFailures),
            Volatile.Read(ref _timeouts),
            Volatile.Read(ref _dropped),
            Volatile.Read(ref _duplicates),
            Volatile.Read(ref _inFlight),
            Volatile.Read(ref _maxInFlight),
            offered == 0 ? 0 : (double)Volatile.Read(ref _totalLines) / offered,
            offered == 0 ? 0 : (double)Volatile.Read(ref _totalUnits) / offered,
            latencies.Length == 0 ? 0 : latencies.Average(),
            Percentile(latencies, 0.50),
            Percentile(latencies, 0.95),
            Percentile(latencies, 0.99),
            _statusCodes.OrderBy(pair => pair.Key).ToDictionary(),
            _locations.OrderBy(pair => pair.Key).ToDictionary());
    }

    public static double Percentile(IReadOnlyList<double> sortedValues, double percentile)
    {
        if (sortedValues.Count == 0)
        {
            return 0;
        }

        var rank = Math.Clamp((int)Math.Ceiling(percentile * sortedValues.Count) - 1, 0, sortedValues.Count - 1);
        return sortedValues[rank];
    }

    public static string FormatSummary(LoadRunSnapshot snapshot, OrderGeneratorOptions options, int skuCount, int? cycle = null)
    {
        var text = new StringBuilder()
            .AppendLine(cycle is null ? "=== ORDER LOAD SUMMARY ===" : $"=== ORDER LOAD SUMMARY: CYCLE {cycle} ===")
            .AppendLine($"Target:             {options.RatePerSecond:F2} req/s ({options.RatePerSecond * 60:F0}/min) for {options.DurationSeconds:F1} s")
            .AppendLine($"Configuration:      profile={options.Profile}, continuous={options.Continuous}, variableRate={options.VariableRate}, waveAmplitude={options.WaveAmplitudeFraction:P0}, wavePeriod={options.WavePeriodSeconds:F0}s, concurrency={options.MaxConcurrency}, seed={options.RandomSeed}, SKUs={skuCount}")
            .AppendLine($"Elapsed:             offer={snapshot.OfferWindow.TotalSeconds:F2} s, total={snapshot.TotalElapsed.TotalSeconds:F2} s")
            .AppendLine($"Offered:             {snapshot.Offered}")
            .AppendLine($"Actual offered rate: {snapshot.OfferedPerSecond:F2} req/s")
            .AppendLine($"HTTP accepted:       {snapshot.Accepted} ({snapshot.Accepted / Math.Max(0.001, snapshot.OfferWindow.TotalSeconds):F2}/s)")
            .AppendLine($"HTTP failures:       {snapshot.HttpFailures}")
            .AppendLine($"Timeouts:            {snapshot.Timeouts}")
            .AppendLine($"Dropped arrivals:    {snapshot.Dropped}")
            .AppendLine($"Duplicate IDs:       {snapshot.DuplicateRequestIds}")
            .AppendLine($"In flight:           {snapshot.CurrentInFlight}; max observed={snapshot.MaxInFlight}")
            .AppendLine($"Latency avg/p50:     {snapshot.AverageLatencyMilliseconds:F1}/{snapshot.P50Milliseconds:F1} ms")
            .AppendLine($"Latency p95/p99:     {snapshot.P95Milliseconds:F1}/{snapshot.P99Milliseconds:F1} ms")
            .AppendLine($"Order shape:         {snapshot.AverageLinesPerOrder:F2} lines, {snapshot.AverageUnitsPerOrder:F2} units average")
            .AppendLine($"HTTP statuses:       {FormatCounts(snapshot.StatusCodes)}")
            .AppendLine($"Locations:           {FormatCounts(snapshot.Locations)}");
        return text.ToString();
    }

    private static string FormatCounts<TKey>(IReadOnlyDictionary<TKey, long> counts) where TKey : notnull =>
        counts.Count == 0 ? "none" : string.Join(", ", counts.Select(pair => $"{pair.Key}={pair.Value}"));
}

public sealed record LoadRunSnapshot(
    TimeSpan OfferWindow,
    TimeSpan TotalElapsed,
    long Offered,
    double OfferedPerSecond,
    long Accepted,
    long HttpFailures,
    long Timeouts,
    long Dropped,
    long DuplicateRequestIds,
    int CurrentInFlight,
    int MaxInFlight,
    double AverageLinesPerOrder,
    double AverageUnitsPerOrder,
    double AverageLatencyMilliseconds,
    double P50Milliseconds,
    double P95Milliseconds,
    double P99Milliseconds,
    IReadOnlyDictionary<int, long> StatusCodes,
    IReadOnlyDictionary<string, long> Locations);
