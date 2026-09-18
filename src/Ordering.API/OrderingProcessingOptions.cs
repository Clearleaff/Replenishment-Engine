namespace eShop.Ordering.API;

public sealed class OrderingProcessingOptions
{
    public const string SectionName = "OrderingProcessing";
    public int SimulatedDelayMilliseconds { get; set; } = 10_000;
}
