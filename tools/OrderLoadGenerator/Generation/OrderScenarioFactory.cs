namespace eShop.OrderLoadGenerator.Generation;

public sealed class OrderScenarioFactory
{
    public static readonly string[] SupportedLocations = ["NCR", "BLR", "BOM", "HYD"];

    private readonly CatalogProduct[] _products;
    private readonly int _hotCount;
    private readonly OrderGeneratorOptions _options;
    private readonly Random _random;
    private readonly Func<Guid> _requestIdFactory;
    private readonly Func<DayOfWeek> _dayOfWeekProvider;
    private readonly CatalogProduct? _targetProduct;
    private long _orderNumber;

    public OrderScenarioFactory(
        IEnumerable<CatalogProduct> products,
        OrderGeneratorOptions options,
        Func<Guid>? requestIdFactory = null,
        Func<DayOfWeek>? dayOfWeekProvider = null)
    {
        _products = products.Where(product => product.Id > 0).OrderBy(product => product.Id).ToArray();
        if (_products.Length == 0)
        {
            throw new ArgumentException("At least one usable Catalog product is required.", nameof(products));
        }

        _options = options;
        _random = new Random(options.RandomSeed);
        _requestIdFactory = requestIdFactory ?? Guid.NewGuid;
        _dayOfWeekProvider = dayOfWeekProvider ?? (() => DateTimeOffset.UtcNow.DayOfWeek);
        _hotCount = Math.Clamp((int)Math.Ceiling(_products.Length * options.HotSkuFraction), 1, _products.Length);
        _targetProduct = _products.FirstOrDefault(product => product.Id == options.TargetSkuId);

        if (options.Profile != OrderWorkloadProfile.Normal && _targetProduct is null)
        {
            throw new ArgumentException(
                $"Workload profile {options.Profile} requires Catalog SKU {options.TargetSkuId}.",
                nameof(products));
        }
    }

    public OrderScenario Create()
    {
        var orderNumber = _orderNumber++;
        var lineCount = _random.Next(1, Math.Min(4, _products.Length) + 1);
        var selected = new HashSet<int>();
        var items = new List<GeneratedOrderItem>(lineCount);

        if (ShouldIncludeTarget())
        {
            selected.Add(_targetProduct!.Id);
            items.Add(CreateItem(_targetProduct, SelectTargetQuantity()));
        }

        while (items.Count < lineCount)
        {
            var product = SelectProduct();
            if (!selected.Add(product.Id))
            {
                continue;
            }

            items.Add(CreateItem(product, SelectQuantity()));
        }

        var customerNumber = orderNumber % _options.CustomerPoolSize;
        return new OrderScenario(
            _requestIdFactory(),
            $"load-customer-{customerNumber:D4}",
            $"Load Customer {customerNumber:D4}",
            SelectLocation(),
            items);
    }

    private CatalogProduct SelectProduct()
    {
        var selectHot = _hotCount == _products.Length || _random.NextDouble() < _options.HotTrafficShare;
        var start = selectHot ? 0 : _hotCount;
        var length = selectHot ? _hotCount : _products.Length - _hotCount;
        return _products[start + _random.Next(length)];
    }

    private GeneratedOrderItem CreateItem(CatalogProduct product, int quantity) => new(
        product.Id.ToString(),
        product.Id,
        product.Name,
        product.Price,
        product.Price,
        quantity,
        product.PictureFileName is null ? null : $"/product-images/{product.Id}");

    private bool ShouldIncludeTarget()
    {
        var targetShare = _options.Profile switch
        {
            OrderWorkloadProfile.Normal => 0,
            OrderWorkloadProfile.WeekdaySeasonal => GetDayOfWeek() switch
            {
                DayOfWeek.Sunday => 0.8,
                DayOfWeek.Tuesday => 0.1,
                _ => 0.35
            },
            OrderWorkloadProfile.Promotion => 0.65,
            OrderWorkloadProfile.Viral => 0.9,
            OrderWorkloadProfile.RegionalSpike => 0.9,
            _ => throw new InvalidOperationException($"Unsupported workload profile {_options.Profile}.")
        };
        return _random.NextDouble() < targetShare;
    }

    private DayOfWeek GetDayOfWeek() => _options.SimulatedDayOfWeek >= 0
        ? (DayOfWeek)_options.SimulatedDayOfWeek
        : _dayOfWeekProvider();

    private int SelectTargetQuantity() => _options.Profile switch
    {
        OrderWorkloadProfile.Viral => 3,
        OrderWorkloadProfile.Promotion or OrderWorkloadProfile.RegionalSpike => _random.Next(2, 4),
        _ => SelectQuantity()
    };

    private int SelectQuantity()
    {
        var percentile = _random.Next(100);
        return percentile < 80 ? 1 : percentile < 95 ? 2 : 3;
    }

    private string SelectLocation()
    {
        if (_options.Profile == OrderWorkloadProfile.RegionalSpike)
        {
            return "NCR";
        }

        var percentile = _random.Next(100);
        return percentile switch
        {
            < 40 => "NCR",
            < 65 => "BLR",
            < 85 => "BOM",
            _ => "HYD"
        };
    }
}
