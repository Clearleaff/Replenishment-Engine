namespace eShop.Inventory.API.Model;

public class InventoryLocation
{
    public string Code { get; private set; } = null!;

    public string Name { get; private set; } = null!;

    public bool IsActive { get; private set; }

    public DateTime CreatedAt { get; private set; }

    private InventoryLocation()
    {
    }

    public InventoryLocation(string code, string name)
    {
        if (string.IsNullOrWhiteSpace(code))
        {
            throw new ArgumentException(
                "Location code is required.",
                nameof(code));
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException(
                "Location name is required.",
                nameof(name));
        }

        var normalizedCode = code.Trim().ToUpperInvariant();
        var normalizedName = name.Trim();

        if (normalizedCode.Length > 16)
        {
            throw new ArgumentException(
                "Location code cannot exceed 16 characters.",
                nameof(code));
        }

        if (normalizedName.Length > 100)
        {
            throw new ArgumentException(
                "Location name cannot exceed 100 characters.",
                nameof(name));
        }

        Code = normalizedCode;
        Name = normalizedName;
        IsActive = true;
        CreatedAt = DateTime.UtcNow;
    }
}
