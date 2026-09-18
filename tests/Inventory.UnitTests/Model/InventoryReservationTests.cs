namespace eShop.Inventory.UnitTests.Model;

[TestClass]
public class InventoryReservationTests
{
    [TestMethod]
    public void New_reservation_starts_reserved()
    {
        var reservedAt = DateTime.UtcNow;

        var reservation = new InventoryReservation(
            orderId: 10,
            skuId: 20,
            locationCode: " ncr ",
            quantity: 3,
            reservedAt: reservedAt);

        Assert.AreEqual(10, reservation.OrderId);
        Assert.AreEqual(20, reservation.SkuId);
        Assert.AreEqual("NCR", reservation.LocationCode);
        Assert.AreEqual(3, reservation.Quantity);
        Assert.AreEqual(InventoryReservationStatus.Reserved, reservation.Status);
        Assert.AreEqual(reservedAt, reservation.ReservedAt);
        Assert.IsNull(reservation.CompletedAt);
    }

    [TestMethod]
    public void Reserved_reservation_can_be_committed()
    {
        var reservation = CreateReservation();
        var completedAt = DateTime.UtcNow;

        reservation.Commit(completedAt);

        Assert.AreEqual(InventoryReservationStatus.Committed, reservation.Status);
        Assert.AreEqual(completedAt, reservation.CompletedAt);
    }

    [TestMethod]
    public void Reserved_reservation_can_be_released()
    {
        var reservation = CreateReservation();
        var completedAt = DateTime.UtcNow;

        reservation.Release(completedAt);

        Assert.AreEqual(InventoryReservationStatus.Released, reservation.Status);
        Assert.AreEqual(completedAt, reservation.CompletedAt);
    }

    [TestMethod]
    public void Completed_reservation_cannot_transition_again()
    {
        var reservation = CreateReservation();
        reservation.Commit(DateTime.UtcNow);

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            reservation.Release(DateTime.UtcNow));
    }

    [TestMethod]
    public void Reservation_quantity_must_be_positive()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new InventoryReservation(
                orderId: 10,
                skuId: 20,
                locationCode: "NCR",
                quantity: 0,
                reservedAt: DateTime.UtcNow));
    }

    private static InventoryReservation CreateReservation() =>
        new(
            orderId: 10,
            skuId: 20,
            locationCode: "NCR",
            quantity: 3,
            reservedAt: DateTime.UtcNow);
}
