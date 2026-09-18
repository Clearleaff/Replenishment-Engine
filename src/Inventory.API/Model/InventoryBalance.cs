namespace eShop.Inventory.API.Model;

public class InventoryBalance
{
    public int SkuId { get; private set; }

    public string LocationCode { get; private set; } = null!;

    public int OnHand { get; private set; }

    public int Reserved { get; private set; }

    public int Available => OnHand - Reserved;

    public int SafetyStock { get; private set; }

    public int ReorderPoint { get; private set; }

    public int MaxStock { get; private set; }

    public long Version { get; private set; }

    public DateTime UpdatedAt { get; private set; }

    private InventoryBalance()
    {
    }

    public InventoryBalance(
        int skuId,
        string locationCode,
        int onHand,
        int reserved,
        int safetyStock,
        int reorderPoint,
        int maxStock)
    {
        if (skuId <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(skuId),
                "SKU ID must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(locationCode))
        {
            throw new ArgumentException(
                "Location code is required.",
                nameof(locationCode));
        }

        if (onHand < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(onHand),
                "On-hand quantity cannot be negative.");
        }

        if (reserved < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(reserved),
                "Reserved quantity cannot be negative.");
        }

        if (reserved > onHand)
        {
            throw new ArgumentException(
                "Reserved quantity cannot exceed on-hand quantity.",
                nameof(reserved));
        }

        if (safetyStock < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(safetyStock),
                "Safety stock cannot be negative.");
        }

        if (reorderPoint < safetyStock)
        {
            throw new ArgumentException(
                "Reorder point cannot be lower than safety stock.",
                nameof(reorderPoint));
        }

        if (maxStock < reorderPoint)
        {
            throw new ArgumentException(
                "Maximum stock cannot be lower than the reorder point.",
                nameof(maxStock));
        }

        if (onHand > maxStock)
        {
            throw new ArgumentException(
                "On-hand quantity cannot exceed maximum stock.",
                nameof(onHand));
        }

        var normalizedLocationCode = locationCode.Trim().ToUpperInvariant();

        if (normalizedLocationCode.Length > 16)
        {
            throw new ArgumentException(
                "Location code cannot exceed 16 characters.",
                nameof(locationCode));
        }

        SkuId = skuId;
        LocationCode = normalizedLocationCode;
        OnHand = onHand;
        Reserved = reserved;
        SafetyStock = safetyStock;
        ReorderPoint = reorderPoint;
        MaxStock = maxStock;
        Version = 1;
        UpdatedAt = DateTime.UtcNow;
    }

    public void Reserve(int quantity, DateTime changedAt)
    {
        EnsurePositive(quantity);
        EnsureUtc(changedAt);
        if (quantity > Available)
        {
            throw new InvalidOperationException("Insufficient available inventory.");
        }

        Reserved += quantity;
        Touch(changedAt);
    }

    public void CommitSale(int quantity, DateTime changedAt)
    {
        EnsurePositive(quantity);
        EnsureUtc(changedAt);
        if (quantity > Reserved)
        {
            throw new InvalidOperationException("Cannot sell more than the reserved quantity.");
        }

        OnHand -= quantity;
        Reserved -= quantity;
        Touch(changedAt);
    }

    public void Release(int quantity, DateTime changedAt)
    {
        EnsurePositive(quantity);
        EnsureUtc(changedAt);
        if (quantity > Reserved)
        {
            throw new InvalidOperationException("Cannot release more than the reserved quantity.");
        }

        Reserved -= quantity;
        Touch(changedAt);
    }

    public int Restock(int quantity, DateTime changedAt)
    {
        EnsurePositive(quantity);
        EnsureUtc(changedAt);
        var accepted = Math.Min(quantity, MaxStock - OnHand);
        if (accepted > 0)
        {
            OnHand += accepted;
            Touch(changedAt);
        }

        return accepted;
    }

    private void Touch(DateTime changedAt)
    {
        Version++;
        UpdatedAt = changedAt;
    }

    private static void EnsurePositive(int quantity)
    {
        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), "Quantity must be greater than zero.");
        }
    }

    private static void EnsureUtc(DateTime value)
    {
        if (value.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException("Timestamp must use UTC.", nameof(value));
        }
    }
}
