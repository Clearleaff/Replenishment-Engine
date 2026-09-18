namespace eShop.Inventory.UnitTests.Model;

[TestClass]
public class InventoryMovementTests
{
    [TestMethod]
    public void Valid_reserve_movement_records_trace_information()
    {
        var movementId = Guid.NewGuid();
        var sourceEventId = Guid.NewGuid();
        var occurredAt = DateTime.UtcNow;

        var movement = new InventoryMovement(
            movementId,
            sourceEventId,
            skuId: 1,
            locationCode: " ncr ",
            orderId: 10,
            movementType: InventoryMovementType.Reserve,
            quantity: 3,
            occurredAt: occurredAt,
            balanceVersionAfter: 2,
            reason: " Customer order ");

        Assert.AreEqual(movementId, movement.MovementId);
        Assert.AreEqual(sourceEventId, movement.SourceEventId);
        Assert.AreEqual("NCR", movement.LocationCode);
        Assert.AreEqual(InventoryMovementType.Reserve, movement.MovementType);
        Assert.AreEqual(3, movement.Quantity);
        Assert.AreEqual(occurredAt, movement.OccurredAt);
        Assert.AreEqual(DateTimeKind.Utc, movement.RecordedAt.Kind);
        Assert.AreEqual(2L, movement.BalanceVersionAfter);
        Assert.AreEqual("Customer order", movement.Reason);
    }

    [TestMethod]
    public void Adjustment_can_use_negative_quantity()
    {
        var movement = CreateMovement(
            InventoryMovementType.Adjustment,
            quantity: -2);

        Assert.AreEqual(-2, movement.Quantity);
    }

    [TestMethod]
    public void Non_adjustment_quantity_must_be_positive()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            CreateMovement(
                InventoryMovementType.Restock,
                quantity: 0));
    }

    [TestMethod]
    public void Source_event_id_is_required()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryMovement(
                Guid.NewGuid(),
                Guid.Empty,
                skuId: 1,
                locationCode: "NCR",
                orderId: null,
                movementType: InventoryMovementType.Restock,
                quantity: 5,
                occurredAt: DateTime.UtcNow,
                balanceVersionAfter: 2));
    }

    [TestMethod]
    public void Occurred_at_must_use_utc()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryMovement(
                Guid.NewGuid(),
                Guid.NewGuid(),
                skuId: 1,
                locationCode: "NCR",
                orderId: null,
                movementType: InventoryMovementType.Restock,
                quantity: 5,
                occurredAt: DateTime.Now,
                balanceVersionAfter: 2));
    }

    private static InventoryMovement CreateMovement(
        InventoryMovementType movementType,
        int quantity) =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            skuId: 1,
            locationCode: "NCR",
            orderId: null,
            movementType: movementType,
            quantity: quantity,
            occurredAt: DateTime.UtcNow,
            balanceVersionAfter: 2);
}
