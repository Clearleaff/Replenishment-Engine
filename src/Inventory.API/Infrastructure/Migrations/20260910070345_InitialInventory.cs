using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Inventory.API.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InitialInventory : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "inventory");

            migrationBuilder.CreateTable(
                name: "locations",
                schema: "inventory",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    name = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: false),
                    is_active = table.Column<bool>(type: "boolean", nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_locations", x => x.code);
                });

            migrationBuilder.CreateTable(
                name: "inventory_balances",
                schema: "inventory",
                columns: table => new
                {
                    sku_id = table.Column<int>(type: "integer", nullable: false),
                    location_code = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    on_hand = table.Column<int>(type: "integer", nullable: false),
                    reserved = table.Column<int>(type: "integer", nullable: false),
                    safety_stock = table.Column<int>(type: "integer", nullable: false),
                    reorder_point = table.Column<int>(type: "integer", nullable: false),
                    max_stock = table.Column<int>(type: "integer", nullable: false),
                    version = table.Column<long>(type: "bigint", nullable: false),
                    updated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_inventory_balances", x => new { x.sku_id, x.location_code });
                    table.CheckConstraint("ck_inventory_balances_capacity", "on_hand <= max_stock");
                    table.CheckConstraint("ck_inventory_balances_max_stock", "max_stock >= reorder_point");
                    table.CheckConstraint("ck_inventory_balances_on_hand", "on_hand >= 0");
                    table.CheckConstraint("ck_inventory_balances_reorder_point", "reorder_point >= safety_stock");
                    table.CheckConstraint("ck_inventory_balances_reserved", "reserved >= 0");
                    table.CheckConstraint("ck_inventory_balances_reserved_on_hand", "reserved <= on_hand");
                    table.CheckConstraint("ck_inventory_balances_safety_stock", "safety_stock >= 0");
                    table.ForeignKey(
                        name: "FK_inventory_balances_locations_location_code",
                        column: x => x.location_code,
                        principalSchema: "inventory",
                        principalTable: "locations",
                        principalColumn: "code",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "inventory_movements",
                schema: "inventory",
                columns: table => new
                {
                    movement_id = table.Column<Guid>(type: "uuid", nullable: false),
                    source_event_id = table.Column<Guid>(type: "uuid", nullable: false),
                    sku_id = table.Column<int>(type: "integer", nullable: false),
                    location_code = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    order_id = table.Column<int>(type: "integer", nullable: true),
                    movement_type = table.Column<string>(type: "character varying(24)", maxLength: 24, nullable: false),
                    quantity = table.Column<int>(type: "integer", nullable: false),
                    occurred_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    recorded_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    balance_version_after = table.Column<long>(type: "bigint", nullable: false),
                    reason = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_inventory_movements", x => x.movement_id);
                    table.CheckConstraint("ck_inventory_movements_balance_version", "balance_version_after > 0");
                    table.CheckConstraint("ck_inventory_movements_quantity", "quantity <> 0");
                    table.ForeignKey(
                        name: "FK_inventory_movements_inventory_balances_sku_id_location_code",
                        columns: x => new { x.sku_id, x.location_code },
                        principalSchema: "inventory",
                        principalTable: "inventory_balances",
                        principalColumns: new[] { "sku_id", "location_code" },
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "inventory_reservations",
                schema: "inventory",
                columns: table => new
                {
                    order_id = table.Column<int>(type: "integer", nullable: false),
                    sku_id = table.Column<int>(type: "integer", nullable: false),
                    location_code = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    quantity = table.Column<int>(type: "integer", nullable: false),
                    status = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    reserved_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    completed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_inventory_reservations", x => new { x.order_id, x.sku_id, x.location_code });
                    table.CheckConstraint("ck_inventory_reservations_quantity", "quantity > 0");
                    table.ForeignKey(
                        name: "FK_inventory_reservations_inventory_balances_sku_id_location_c~",
                        columns: x => new { x.sku_id, x.location_code },
                        principalSchema: "inventory",
                        principalTable: "inventory_balances",
                        principalColumns: new[] { "sku_id", "location_code" },
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "ix_inventory_balances_location_sku",
                schema: "inventory",
                table: "inventory_balances",
                columns: new[] { "location_code", "sku_id" });

            migrationBuilder.CreateIndex(
                name: "ix_inventory_movements_order_id",
                schema: "inventory",
                table: "inventory_movements",
                column: "order_id");

            migrationBuilder.CreateIndex(
                name: "ix_inventory_movements_recorded_at",
                schema: "inventory",
                table: "inventory_movements",
                column: "recorded_at");

            migrationBuilder.CreateIndex(
                name: "ix_inventory_movements_sku_location_occurred_at",
                schema: "inventory",
                table: "inventory_movements",
                columns: new[] { "sku_id", "location_code", "occurred_at" });

            migrationBuilder.CreateIndex(
                name: "ux_inventory_movements_source_sku_location_type",
                schema: "inventory",
                table: "inventory_movements",
                columns: new[] { "source_event_id", "sku_id", "location_code", "movement_type" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_inventory_reservations_sku_id_location_code",
                schema: "inventory",
                table: "inventory_reservations",
                columns: new[] { "sku_id", "location_code" });

            migrationBuilder.CreateIndex(
                name: "ix_inventory_reservations_status_reserved_at",
                schema: "inventory",
                table: "inventory_reservations",
                columns: new[] { "status", "reserved_at" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "inventory_movements",
                schema: "inventory");

            migrationBuilder.DropTable(
                name: "inventory_reservations",
                schema: "inventory");

            migrationBuilder.DropTable(
                name: "inventory_balances",
                schema: "inventory");

            migrationBuilder.DropTable(
                name: "locations",
                schema: "inventory");
        }
    }
}
