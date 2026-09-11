using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace eShop.Catalog.API.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class RemoveCatalogStockOwnership : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "AvailableStock",
                table: "Catalog");

            migrationBuilder.DropColumn(
                name: "MaxStockThreshold",
                table: "Catalog");

            migrationBuilder.DropColumn(
                name: "OnReorder",
                table: "Catalog");

            migrationBuilder.DropColumn(
                name: "RestockThreshold",
                table: "Catalog");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "AvailableStock",
                table: "Catalog",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "MaxStockThreshold",
                table: "Catalog",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<bool>(
                name: "OnReorder",
                table: "Catalog",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<int>(
                name: "RestockThreshold",
                table: "Catalog",
                type: "integer",
                nullable: false,
                defaultValue: 0);
        }
    }
}
