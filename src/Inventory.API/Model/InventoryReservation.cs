namespace eShop.Inventory.API.Model;

public class InventoryReservation
{
    public int OrderId { get; private set; }

    public int SkuId { get; private set; }

    public string LocationCode { get; private set; } = null!;

    public int Quantity { get; private set; }

    public InventoryReservationStatus Status { get; private set; }

    public DateTime ReservedAt { get; private set; }

    public DateTime? CompletedAt { get; private set; }

    private InventoryReservation()
    {
    }

    public InventoryReservation(
        int orderId,
        int skuId,
        string locationCode,
        int quantity,
        DateTime reservedAt)
    {
        if (orderId <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(orderId),
                "Order ID must be greater than zero.");
        }

        if (skuId <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(skuId),
                "SKU ID must be greater than zero.");
        }

        LocationCode = NormalizeLocationCode(locationCode);

        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(quantity),
                "Reservation quantity must be greater than zero.");
        }

        EnsureUtc(reservedAt, nameof(reservedAt));

        OrderId = orderId;
        SkuId = skuId;
        Quantity = quantity;
        Status = InventoryReservationStatus.Reserved;
        ReservedAt = reservedAt;
    }

    public void Commit(DateTime completedAt)
    {
        EnsureCanComplete();
        EnsureUtc(completedAt, nameof(completedAt));

        Status = InventoryReservationStatus.Committed;
        CompletedAt = completedAt;
    }

    public void Release(DateTime completedAt)
    {
        EnsureCanComplete();
        EnsureUtc(completedAt, nameof(completedAt));

        Status = InventoryReservationStatus.Released;
        CompletedAt = completedAt;
    }

    private void EnsureCanComplete()
    {
        if (Status != InventoryReservationStatus.Reserved)
        {
            throw new InvalidOperationException(
                $"A {Status} reservation cannot be changed.");
        }
    }

    private static string NormalizeLocationCode(string locationCode)
    {
        if (string.IsNullOrWhiteSpace(locationCode))
        {
            throw new ArgumentException(
                "Location code is required.",
                nameof(locationCode));
        }

        var normalizedCode = locationCode.Trim().ToUpperInvariant();

        if (normalizedCode.Length > 16)
        {
            throw new ArgumentException(
                "Location code cannot exceed 16 characters.",
                nameof(locationCode));
        }

        return normalizedCode;
    }

    private static void EnsureUtc(DateTime value, string parameterName)
    {
        if (value.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException(
                "Timestamp must use UTC.",
                parameterName);
        }
    }
}
