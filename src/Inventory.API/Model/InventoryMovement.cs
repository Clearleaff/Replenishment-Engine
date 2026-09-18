namespace eShop.Inventory.API.Model;

public class InventoryMovement
{
    public Guid MovementId { get; private set; }

    public Guid SourceEventId { get; private set; }

    public int SkuId { get; private set; }

    public string LocationCode { get; private set; } = null!;

    public int? OrderId { get; private set; }

    public InventoryMovementType MovementType { get; private set; }

    public int Quantity { get; private set; }

    public DateTime OccurredAt { get; private set; }

    public DateTime RecordedAt { get; private set; }

    public long BalanceVersionAfter { get; private set; }

    public string? Reason { get; private set; }

    private InventoryMovement()
    {
    }

    public InventoryMovement(
        Guid movementId,
        Guid sourceEventId,
        int skuId,
        string locationCode,
        int? orderId,
        InventoryMovementType movementType,
        int quantity,
        DateTime occurredAt,
        long balanceVersionAfter,
        string? reason = null)
    {
        if (movementId == Guid.Empty)
        {
            throw new ArgumentException(
                "Movement ID is required.",
                nameof(movementId));
        }

        if (sourceEventId == Guid.Empty)
        {
            throw new ArgumentException(
                "Source event ID is required.",
                nameof(sourceEventId));
        }

        if (skuId <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(skuId),
                "SKU ID must be greater than zero.");
        }

        if (orderId <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(orderId),
                "Order ID must be greater than zero when provided.");
        }

        if (!Enum.IsDefined(movementType))
        {
            throw new ArgumentOutOfRangeException(
                nameof(movementType),
                "Movement type is not valid.");
        }

        if (movementType == InventoryMovementType.Adjustment)
        {
            if (quantity == 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(quantity),
                    "Adjustment quantity cannot be zero.");
            }
        }
        else if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(quantity),
                "Movement quantity must be greater than zero.");
        }

        if (occurredAt.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException(
                "Occurred-at timestamp must use UTC.",
                nameof(occurredAt));
        }

        if (balanceVersionAfter <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(balanceVersionAfter),
                "Resulting balance version must be greater than zero.");
        }

        var normalizedLocationCode = NormalizeLocationCode(locationCode);
        var normalizedReason = reason?.Trim();

        if (normalizedReason?.Length > 200)
        {
            throw new ArgumentException(
                "Movement reason cannot exceed 200 characters.",
                nameof(reason));
        }

        MovementId = movementId;
        SourceEventId = sourceEventId;
        SkuId = skuId;
        LocationCode = normalizedLocationCode;
        OrderId = orderId;
        MovementType = movementType;
        Quantity = quantity;
        OccurredAt = occurredAt;
        RecordedAt = DateTime.UtcNow;
        BalanceVersionAfter = balanceVersionAfter;
        Reason = string.IsNullOrEmpty(normalizedReason) ? null : normalizedReason;
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
}
