namespace eShop.Inventory.UnitTests.Model;

[TestClass]
public class InventoryLocationTests
{
    [TestMethod]
    public void Valid_location_is_normalized_and_active()
    {
        var location = new InventoryLocation(
            code: " ncr ",
            name: " National Capital Region ");

        Assert.AreEqual("NCR", location.Code);
        Assert.AreEqual("National Capital Region", location.Name);
        Assert.IsTrue(location.IsActive);
        Assert.AreEqual(DateTimeKind.Utc, location.CreatedAt.Kind);
    }

    [TestMethod]
    public void Location_code_is_required()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryLocation(
                code: " ",
                name: "National Capital Region"));
    }

    [TestMethod]
    public void Location_name_is_required()
    {
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InventoryLocation(
                code: "NCR",
                name: ""));
    }
}
