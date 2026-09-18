namespace eShop.Inventory.UnitTests.Model;

[TestClass]
public class InventoryBalanceTests
{
    [TestMethod]
    public void Valid_balance_calculates_available_stock()
    {
        var balance = new InventoryBalance(
            skuId: 1,
            locationCode: "ncr",
            onHand: 10,
            reserved: 3,
            safetyStock: 2,
            reorderPoint: 5,
            maxStock: 20);

        Assert.AreEqual(1, balance.SkuId);
        Assert.AreEqual("NCR", balance.LocationCode);
        Assert.AreEqual(10, balance.OnHand);
        Assert.AreEqual(3, balance.Reserved);
        Assert.AreEqual(7, balance.Available);
        Assert.AreEqual(1L, balance.Version);
        Assert.AreEqual(DateTimeKind.Utc, balance.UpdatedAt.Kind);
    }

    [TestMethod]
    public void Reserved_stock_cannot_exceed_on_hand_stock()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryBalance(
                skuId: 1,
                locationCode: "NCR",
                onHand: 5,
                reserved: 6,
                safetyStock: 1,
                reorderPoint: 2,
                maxStock: 10));
    }

    [TestMethod]
    public void Reorder_point_cannot_be_lower_than_safety_stock()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryBalance(
                skuId: 1,
                locationCode: "NCR",
                onHand: 5,
                reserved: 0,
                safetyStock: 4,
                reorderPoint: 3,
                maxStock: 10));
    }

    [TestMethod]
    public void On_hand_stock_cannot_exceed_maximum_stock()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryBalance(
                skuId: 1,
                locationCode: "NCR",
                onHand: 11,
                reserved: 0,
                safetyStock: 2,
                reorderPoint: 5,
                maxStock: 10));
    }

    [TestMethod]
    public void Sku_id_must_be_positive()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new InventoryBalance(
                skuId: 0,
                locationCode: "NCR",
                onHand: 5,
                reserved: 0,
                safetyStock: 1,
                reorderPoint: 2,
                maxStock: 10));
    }

    [TestMethod]
    public void Reserve_changes_reserved_available_version_and_timestamp()
    {
        var balance = CreateBalance();
        var changedAt = DateTime.UtcNow.AddMinutes(1);

        balance.Reserve(4, changedAt);

        Assert.AreEqual(20, balance.OnHand);
        Assert.AreEqual(7, balance.Reserved);
        Assert.AreEqual(13, balance.Available);
        Assert.AreEqual(2L, balance.Version);
        Assert.AreEqual(changedAt, balance.UpdatedAt);
    }

    [TestMethod]
    public void Reserve_rejects_quantity_above_available()
    {
        var balance = CreateBalance();

        Assert.ThrowsExactly<InvalidOperationException>(() => balance.Reserve(18, DateTime.UtcNow));
        Assert.AreEqual(3, balance.Reserved);
        Assert.AreEqual(1L, balance.Version);
    }

    [TestMethod]
    public void Commit_sale_reduces_on_hand_and_reserved_without_reducing_available_twice()
    {
        var balance = CreateBalance();

        balance.CommitSale(2, DateTime.UtcNow);

        Assert.AreEqual(18, balance.OnHand);
        Assert.AreEqual(1, balance.Reserved);
        Assert.AreEqual(17, balance.Available);
        Assert.AreEqual(2L, balance.Version);
    }

    [TestMethod]
    public void Release_only_reduces_reserved()
    {
        var balance = CreateBalance();

        balance.Release(2, DateTime.UtcNow);

        Assert.AreEqual(20, balance.OnHand);
        Assert.AreEqual(1, balance.Reserved);
        Assert.AreEqual(19, balance.Available);
    }

    [TestMethod]
    public void Restock_caps_at_maximum_stock_and_returns_accepted_quantity()
    {
        var balance = CreateBalance();

        var accepted = balance.Restock(20, DateTime.UtcNow);

        Assert.AreEqual(10, accepted);
        Assert.AreEqual(30, balance.OnHand);
        Assert.AreEqual(27, balance.Available);
        Assert.AreEqual(2L, balance.Version);
    }

    private static InventoryBalance CreateBalance() => new(
        skuId: 1,
        locationCode: "NCR",
        onHand: 20,
        reserved: 3,
        safetyStock: 2,
        reorderPoint: 5,
        maxStock: 30);
}
