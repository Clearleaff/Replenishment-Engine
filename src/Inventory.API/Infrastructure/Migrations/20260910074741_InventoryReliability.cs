using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Inventory.API.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InventoryReliability : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "incoming_integration_events",
                schema: "inventory",
                columns: table => new
                {
                    event_id = table.Column<Guid>(type: "uuid", nullable: false),
                    event_type = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    processed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_incoming_integration_events", x => x.event_id);
                });

            migrationBuilder.CreateTable(
                name: "IntegrationEventLog",
                schema: "inventory",
                columns: table => new
                {
                    EventId = table.Column<Guid>(type: "uuid", nullable: false),
                    EventTypeName = table.Column<string>(type: "text", nullable: false),
                    State = table.Column<int>(type: "integer", nullable: false),
                    TimesSent = table.Column<int>(type: "integer", nullable: false),
                    CreationTime = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    Content = table.Column<string>(type: "text", nullable: false),
                    TransactionId = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IntegrationEventLog", x => x.EventId);
                });

            migrationBuilder.CreateTable(
                name: "inventory_shadow_checks",
                schema: "inventory",
                columns: table => new
                {
                    source_event_id = table.Column<Guid>(type: "uuid", nullable: false),
                    order_id = table.Column<int>(type: "integer", nullable: false),
                    location_code = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    inventory_confirmed = table.Column<bool>(type: "boolean", nullable: false),
                    catalog_confirmed = table.Column<bool>(type: "boolean", nullable: true),
                    details = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: false),
                    evaluated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    catalog_observed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_inventory_shadow_checks", x => x.source_event_id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_inventory_shadow_checks_order_id",
                schema: "inventory",
                table: "inventory_shadow_checks",
                column: "order_id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "incoming_integration_events",
                schema: "inventory");

            migrationBuilder.DropTable(
                name: "IntegrationEventLog",
                schema: "inventory");

            migrationBuilder.DropTable(
                name: "inventory_shadow_checks",
                schema: "inventory");
        }
    }
}
