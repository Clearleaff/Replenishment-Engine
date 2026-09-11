# Inventory Service implementation walkthrough

This is the chronological teaching record for the Inventory Service. It records what changed, why it changed, dependencies, and verification. The canonical code is always the linked repository file; the code blocks below show the code introduced at each step.

## Step 1 — Empty Inventory API shell

### FILE: `src/Inventory.API/Inventory.API.csproj`

**Why it exists:** Every .NET project needs a project file describing its SDK, target framework, packages, and project dependencies.

**Responsibility:** Build Inventory as a .NET 10 ASP.NET Core application.

**Depends on:** `eShop.ServiceDefaults` and `Asp.Versioning.Http`.

**Depended on by:** Inventory source files and `Inventory.UnitTests`.

**Code introduced in Step 1:**

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Asp.Versioning.Http" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\eShop.ServiceDefaults\eShop.ServiceDefaults.csproj" />
  </ItemGroup>
</Project>
```

**Sequential explanation:** `Microsoft.NET.Sdk.Web` enables ASP.NET Core hosting. The property group selects .NET 10, enables null-safety analysis, and imports common namespaces. The package enables repository-style API versioning. The project reference reuses eShop health, telemetry, OpenAPI, resilience, and service-discovery defaults.

### FILE: `src/Inventory.API/Program.cs`

**Why it exists:** This is the process entry point.

**Responsibility:** Build the dependency-injection container and HTTP pipeline, then start Kestrel.

**Depends on:** ASP.NET Core and `eShop.ServiceDefaults`.

**Depended on by:** Runtime hosting and test hosting.

**Code after Step 1:**

```csharp
var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddProblemDetails();
var withApiVersioning = builder.Services.AddApiVersioning(options =>
{
    options.ReportApiVersions = true;
});
builder.AddDefaultOpenApi(withApiVersioning);
var app = builder.Build();
app.MapDefaultEndpoints();
app.UseStatusCodePages();
app.UseDefaultOpenApi();
app.Run();
```

**Sequential explanation:** The builder loads configuration, logging, DI, and hosting. Service defaults register common eShop infrastructure. Problem Details standardizes errors. API versioning reports supported versions. OpenAPI registration supplies machine-readable docs and Scalar. `Build` creates the app. Default endpoints expose `/health` and `/alive` in Development. Status pages provide bodies for HTTP errors. OpenAPI middleware exposes docs. `Run` starts the server.

### FILE: `src/Inventory.API/GlobalUsings.cs`

```csharp
global using Asp.Versioning;
global using Asp.Versioning.Conventions;
global using eShop.ServiceDefaults;
```

Each `global using` makes a namespace available to all project files. The first two support versioned APIs; the last exposes shared eShop extension methods.

### FILE: `src/Inventory.API/Program.Testing.cs`

```csharp
public partial class Program
{
}
```

The top-level statements generate an internal `Program`. This public partial declaration makes that entry point discoverable by ASP.NET test hosts without adding runtime behavior.

### FILE: `src/Inventory.API/appsettings.json`

```json
{
  "Logging": { "LogLevel": { "Default": "Information", "Microsoft.AspNetCore": "Warning" } },
  "OpenApi": {
    "Document": {
      "Description": "The Inventory Microservice HTTP API.",
      "Title": "eShop - Inventory HTTP API"
    }
  }
}
```

The logging block keeps normal application information while reducing framework noise. The OpenAPI block activates the shared documentation helper and supplies its title and description.

### FILE: `src/Inventory.API/appsettings.Development.json`

```json
{
  "Logging": { "LogLevel": { "Default": "Information", "Microsoft.AspNetCore": "Information" } }
}
```

Development configuration overlays the base configuration and makes ASP.NET request logs visible.

### FILE: `src/Inventory.API/Properties/launchSettings.json`

```json
{
  "profiles": {
    "http": {
      "commandName": "Project",
      "launchBrowser": true,
      "applicationUrl": "http://localhost:5225/",
      "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
    }
  }
}
```

The `http` profile launches this project on unused port 5225. Development mode enables health and Scalar endpoints. This file is for local tooling, not production deployment.

### FILE: `eShop.slnx`

**Changed line:**

```xml
<Project Path="src/Inventory.API/Inventory.API.csproj" />
```

This makes solution-wide commands and IDEs aware of Inventory API. It does not start the service; AppHost registration is separate.

**Step 1 verification:** Inventory built, listened on port 5225, returned `200` from `/health` and `/alive`, and redirected `/` to `/scalar/v1`.

## Step 2 — Location and balance model

### FILE: `src/Inventory.API/Model/InventoryLocation.cs`

**Why it exists:** Inventory needs a controlled local representation of a distribution center.

**Responsibility:** Hold a normalized code, display name, active flag, and UTC creation time.

**Depends on:** Basic .NET types only.

**Depended on by:** Balance persistence and later seed data.

**Meaningful code:**

```csharp
public class InventoryLocation
{
    public string Code { get; private set; } = null!;
    public string Name { get; private set; } = null!;
    public bool IsActive { get; private set; }
    public DateTime CreatedAt { get; private set; }

    private InventoryLocation() { }

    public InventoryLocation(string code, string name)
    {
        // Reject blank values, trim both strings, uppercase the code,
        // enforce code/name limits, activate it, and timestamp it in UTC.
    }
}
```

**Sequential explanation:** Private setters prevent arbitrary mutation. `null!` satisfies compile-time initialization because EF can use the private constructor. Application callers use the public constructor. It rejects blank input, normalizes ` ncr ` to `NCR`, enforces 16/100-character limits, starts active, and records UTC. The complete executable constructor is in the linked source file.

### FILE: `src/Inventory.API/Model/InventoryBalance.cs`

**Why it exists:** This is current inventory state for one SKU at one location.

**Responsibility:** Hold stock quantities and replenishment policy while enforcing valid initial state.

**Key code:**

```csharp
public int SkuId { get; private set; }
public string LocationCode { get; private set; } = null!;
public int OnHand { get; private set; }
public int Reserved { get; private set; }
public int Available => OnHand - Reserved;
public int SafetyStock { get; private set; }
public int ReorderPoint { get; private set; }
public int MaxStock { get; private set; }
public long Version { get; private set; }
public DateTime UpdatedAt { get; private set; }
```

**Sequential explanation:** SKU and location form the natural identity. On-hand is physical stock; reserved is promised stock. Available is calculated so it cannot drift. Safety stock, reorder point, and max stock are deterministic policy inputs. Version begins at 1 for concurrency checks. UpdatedAt uses UTC. The constructor rejects non-positive SKU IDs, blanks, negative quantities, reserved above on-hand, invalid policy ordering, and on-hand above capacity, then normalizes the location code.

### FILES: `tests/Inventory.UnitTests/Inventory.UnitTests.csproj` and `GlobalUsings.cs`

The MSTest project targets .NET 10 and references Inventory API. Global imports expose `System`, the Inventory model namespace, and MSTest. Method-level parallelization keeps independent tests fast.

### FILES: `InventoryBalanceTests.cs` and `InventoryLocationTests.cs`

The tests create real domain objects and assert normalization, calculated availability, UTC timestamps, positive SKU identity, valid stock relationships, valid replenishment-policy ordering, required location fields, and maximum capacity. Every invalid test uses `Assert.ThrowsExactly` so both the rejection and exception type are contractual.

### FILE: `eShop.slnx`

```xml
<Project Path="tests/Inventory.UnitTests/Inventory.UnitTests.csproj" />
```

This added the new test project to the solution.

## Step 3 — Reservation and movement model

### FILE: `InventoryReservationStatus.cs`

```csharp
public enum InventoryReservationStatus
{
    Reserved = 1,
    Committed = 2,
    Released = 3
}
```

Explicit values make persisted meaning stable. Reserved owns stock temporarily; Committed became a sale; Released returned availability.

### FILE: `InventoryMovementType.cs`

```csharp
public enum InventoryMovementType
{
    Reserve = 1, Release = 2, Sale = 3,
    Restock = 4, Return = 5, Adjustment = 6
}
```

These names describe why inventory changed and will later be stored as readable strings.

### FILE: `InventoryReservation.cs`

**Responsibility:** Track one order/SKU/location claim and its legal state transition.

```csharp
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
```

The constructor validates positive order/SKU/quantity, normalized location, and UTC reservation time. Both transitions first require current status `Reserved`, then require UTC, then set the terminal state and time. A terminal reservation cannot transition again.

### FILE: `InventoryMovement.cs`

**Responsibility:** Represent an append-only, traceable inventory fact.

The constructor validates non-empty movement/source IDs, positive SKU, a positive order ID when present, a defined type, non-zero quantity, UTC event time, positive resulting version, normalized location, and a reason no longer than 200 characters. Normal movement quantities must be positive; only Adjustment may be negative. `OccurredAt` is business time, while `RecordedAt` is Inventory processing time.

### FILES: `InventoryReservationTests.cs` and `InventoryMovementTests.cs`

Reservation tests cover creation, commit, release, repeated transitions, and quantity validation. Movement tests cover trace IDs, normalization, reason trimming, negative adjustments, normal positive-quantity rules, required source IDs, and UTC event time.

**Step 3 verification:** 18 total model tests passed.

## Step 4 — EF Core context and mappings

### FILE: `src/Inventory.API/Inventory.API.csproj`

**Before:** Only API versioning and ServiceDefaults were referenced.

**Added code:**

```xml
<PackageReference Include="Aspire.Npgsql.EntityFrameworkCore.PostgreSQL" />
<PackageReference Include="Microsoft.EntityFrameworkCore.Tools">
  <PrivateAssets>all</PrivateAssets>
  <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
</PackageReference>
```

The Aspire package supplies Npgsql EF registration, health checks, and telemetry. EF Tools enables migrations. `PrivateAssets=all` prevents tooling from becoming a dependency exposed to consumers; IncludeAssets keeps its build/design-time functionality locally available.

### FILE: `src/Inventory.API/GlobalUsings.cs`

**Added lines:**

```csharp
global using eShop.Inventory.API.Infrastructure;
global using eShop.Inventory.API.Infrastructure.EntityConfigurations;
global using eShop.Inventory.API.Model;
global using Microsoft.EntityFrameworkCore;
global using Microsoft.EntityFrameworkCore.Metadata.Builders;
```

In order: expose the context, mapping classes, domain entities, EF runtime types, and EF mapping-builder types project-wide.

### FILE: `src/Inventory.API/Extensions/Extensions.cs`

```csharp
internal static class Extensions
{
    public static void AddApplicationServices(this IHostApplicationBuilder builder)
    {
        builder.AddNpgsqlDbContext<InventoryContext>("inventorydb");
    }
}
```

This repository-style extension keeps startup concise. `internal` limits it to Inventory. The extension receives the host builder and registers `InventoryContext` using the future `inventorydb` connection-string name. It does not create a database or modify AppHost.

### FILE: `src/Inventory.API/Program.cs`

**Added line:**

```csharp
builder.AddApplicationServices();
```

This calls the preceding registration before `Build`, making the context available through dependency injection.

### FILE: `src/Inventory.API/Infrastructure/InventoryContext.cs`

```csharp
public class InventoryContext(DbContextOptions<InventoryContext> options) : DbContext(options)
{
    public DbSet<InventoryLocation> Locations => Set<InventoryLocation>();
    public DbSet<InventoryBalance> InventoryBalances => Set<InventoryBalance>();
    public DbSet<InventoryReservation> InventoryReservations => Set<InventoryReservation>();
    public DbSet<InventoryMovement> InventoryMovements => Set<InventoryMovement>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("inventory");
        modelBuilder.ApplyConfiguration(new InventoryLocationEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryBalanceEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryReservationEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryMovementEntityTypeConfiguration());
    }
}
```

The primary constructor receives configured EF options and passes them to `DbContext`. Each `DbSet` is a query/write gateway for one entity. `OnModelCreating` selects the `inventory` PostgreSQL schema, then applies each focused mapping. This context is the service-owned database boundary.

### FILE: `InventoryLocationEntityTypeConfiguration.cs`

Maps the entity to `inventory.locations`; makes `code` the non-generated primary key; maps lowercase column names; applies 16/100-character limits; and requires active state and creation time. It depends on `InventoryLocation` and EF's `EntityTypeBuilder`; `InventoryContext` depends on this mapping.

### FILE: `InventoryBalanceEntityTypeConfiguration.cs`

Maps `inventory.inventory_balances`. It adds database checks for non-negative quantities, reserved not exceeding on-hand, valid safety/reorder/max ordering, and capacity. The composite primary key is `(sku_id, location_code)`. `Available` is ignored because it is computed. `version` is an optimistic concurrency token. A restricted location foreign key prevents deleting a referenced DC. The `(location_code, sku_id)` index supports DC-first queries.

### FILE: `InventoryReservationEntityTypeConfiguration.cs`

Maps `inventory.inventory_reservations`; enforces positive quantity; uses `(order_id, sku_id, location_code)` as its key; stores status as readable text; links to the composite balance key with restricted deletion; and indexes status plus reservation time for finding live/old reservations.

### FILE: `InventoryMovementEntityTypeConfiguration.cs`

Maps `inventory.inventory_movements`; uses application-supplied UUIDs; stores movement type as text; enforces non-zero quantity and positive resulting version; restricts deletion of referenced balances; prevents duplicate `(source_event_id, sku_id, location_code, movement_type)` facts; and adds SKU/time, order, and recorded-time indexes for reconciliation and future CDC analytics.

**Step 4 verification:** Inventory API built with zero warnings/errors. All 18 domain tests passed with zero failures. No migration or database was created in this step.

## Required human checkpoint rule read before Step 5

The repository `AGENTS.md` explicitly says:

> "Stop and wait for the user to make the change, run the command, and share the result before continuing to the next meaningful step."

It also says:

> "Do not advance to the next phase until the current phase has a clear success check and the user has run it successfully."

Therefore Codex may implement and locally verify one step, but must stop so the learner can run its verification command before the next meaningful step begins.

## Step 5 — Initial EF Core migration

### Concept: EF Core migration

An EF migration is a versioned C# description of how to move a database schema forward or backward. EF compares the current model with its last snapshot and generates operations such as `CreateTable`, `CreateIndex`, and `AddForeignKey`. This makes schema creation repeatable instead of relying on manually typed SQL.

No Inventory database was created or changed in this step. We generated C# and inspected the SQL it would execute later.

### Repository-compatible generation approach

Repository comments establish two patterns:

- Catalog and Webhooks keep their `DbContext` and startup in one API project, so they run `dotnet ef migrations add` from that project.
- Ordering keeps its context in `Ordering.Infrastructure`, so it additionally specifies `--startup-project Ordering.API`.

Inventory follows the first pattern because `InventoryContext` and `Program.cs` both live in `Inventory.API`.

The repository did not contain a local tool manifest and `dotnet ef --version` confirmed the CLI was absent. Version 10.0.11, matching the project EF packages, was installed only under `/tmp/eshop-dotnet-tools`; this did not add a repository or global tool.

### Command used

```bash
ASPNETCORE_ENVIRONMENT=Development \
ConnectionStrings__inventorydb='Host=localhost;Database=inventorydb;Username=postgres;Password=not-used-for-scaffolding' \
/tmp/eshop-dotnet-tools/dotnet-ef migrations add InitialInventory \
  --context InventoryContext \
  --output-dir Infrastructure/Migrations
```

The temporary connection string lets the design-time host configure Npgsql. Migration scaffolding reads model metadata and does not connect to or create that database.

### FILE: `src/Inventory.API/Infrastructure/Migrations/20260910070345_InitialInventory.cs`

**WHY:** This is the executable forward/backward schema change.

**BEFORE:** Inventory entities and mappings existed only as C# metadata.

**AFTER:** A later migration runner can create the exact PostgreSQL schema from an empty database.

**DEPENDS ON:** EF Core migrations and the Step 4 entity configurations.

**DEPENDED ON BY:** The later Inventory database migration/startup process.

**Meaningful generated code, in execution order:**

```csharp
migrationBuilder.EnsureSchema(name: "inventory");

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
    constraints: table => table.PrimaryKey("PK_locations", x => x.code));
```

`EnsureSchema` creates the service-owned namespace when missing. `locations` is created first because balances reference it. Its string code is the natural primary key.

```csharp
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
    });
```

The generated constraint section makes `(sku_id, location_code)` the primary key, restricts its location foreign key, and repeats all stock/policy invariants as PostgreSQL checks. This provides a second safety layer even if a future caller bypasses the C# constructor.

```csharp
migrationBuilder.CreateTable(name: "inventory_movements", schema: "inventory", ...);
migrationBuilder.CreateTable(name: "inventory_reservations", schema: "inventory", ...);
```

Movements use `movement_id` as their primary key and reference a balance using `(sku_id, location_code)`. Reservations use `(order_id, sku_id, location_code)` as their logical primary key and also reference the composite balance. Both relationships use `RESTRICT`, protecting history from accidental parent deletion.

```csharp
migrationBuilder.CreateIndex(
    name: "ux_inventory_movements_source_sku_location_type",
    schema: "inventory",
    table: "inventory_movements",
    columns: new[] { "source_event_id", "sku_id", "location_code", "movement_type" },
    unique: true);
```

This unique index is the movement-level idempotency guard: the same source operation cannot append the same movement type twice for one SKU/location.

Additional generated indexes support location-first balance access, movement queries by order/time/SKU-location, reservation foreign-key lookup, and reservation-status scans.

The generated `Down` method drops children first—movements, reservations, balances—then locations. That order respects foreign keys during rollback.

**DATA FLOW:** EF migration runner → migration `Up` → PostgreSQL schema/tables/constraints/indexes. Normal API requests do not call this class.

**FAILURE MODE:** Without this migration, environments could have different hand-created schemas. Wrong keys could merge locations, missing checks could permit corrupt stock, and missing uniqueness could allow duplicate movements.

### FILE: `src/Inventory.API/Infrastructure/Migrations/20260910070345_InitialInventory.Designer.cs`

**WHY:** EF generates the target model paired with this specific migration.

**CODE CHANGED:** The generated `BuildTargetModel` records provider version 10.0.11, default schema `inventory`, every mapped property/type, the balance and reservation composite keys, all indexes/checks, and all three restricted foreign-key relationships.

**SEQUENTIAL EXPLANATION:** EF first records model/provider metadata, then describes Balance, Location, Movement, and Reservation properties and keys, then describes their relationships. This file should be regenerated rather than manually maintained.

**FAILURE MODE:** If this file diverges from its migration, later EF tooling can misunderstand the model associated with this migration.

### FILE: `src/Inventory.API/Infrastructure/Migrations/InventoryContextModelSnapshot.cs`

**WHY:** This is EF's latest-model baseline for calculating the next migration.

**BEFORE:** No Inventory snapshot existed, so EF treated the whole model as new.

**AFTER:** A future change is compared against this snapshot, producing only the schema delta.

**CODE CHANGED:** `BuildModel` contains the same current entity properties, PostgreSQL types, keys, checks, indexes, conversions, schema, and relationships as the migration designer.

**FAILURE MODE:** Deleting or hand-editing the snapshot can cause the next migration to recreate, drop, or alter the wrong objects.

### Generated SQL inspection

After rebuilding so the new migration was present in the compiled assembly, SQL was generated to `/tmp/inventory-initial.sql` with:

```bash
ASPNETCORE_ENVIRONMENT=Development \
ConnectionStrings__inventorydb='Host=localhost;Database=inventorydb;Username=postgres;Password=not-used-for-scaffolding' \
/tmp/eshop-dotnet-tools/dotnet-ef migrations script \
  0 20260910070345_InitialInventory \
  --context InventoryContext --no-build \
  --output /tmp/inventory-initial.sql
```

Inspection confirmed `CREATE SCHEMA inventory`, all four tables, both composite primary keys, three foreign-key relationships, seven balance checks, reservation/movement checks, and the unique movement idempotency index. The shortened generated reservation FK name ending in `~` is EF's normal handling of PostgreSQL's 63-character identifier limit.

### CHECKPOINT RESULT

**Status:** PASS

**Commands actually run:**

```text
dotnet ef --version
dotnet tool install --tool-path /tmp/eshop-dotnet-tools dotnet-ef --version 10.0.11
dotnet-ef migrations add InitialInventory --context InventoryContext --output-dir Infrastructure/Migrations
dotnet build src/Inventory.API/Inventory.API.csproj --no-restore
dotnet-ef migrations script 0 20260910070345_InitialInventory --context InventoryContext --no-build
dotnet test tests/Inventory.UnitTests/Inventory.UnitTests.csproj --no-restore
```

**Build:** 0 warnings, 0 errors.

**Tests:** 18 succeeded, 0 failed, 0 skipped.

**Important observation:** The first script attempt used `--no-build` before the new migration had been compiled, so EF could not find it. Rebuilding and targeting its full migration ID produced the expected SQL. No database was contacted or modified.

**How the learner can verify:**

```bash
dotnet build src/Inventory.API/Inventory.API.csproj --no-restore
dotnet test tests/Inventory.UnitTests/Inventory.UnitTests.csproj --no-restore
```

## Step 6 — Deterministic seed data

### FILE: `src/Inventory.API/Inventory.API.csproj`

**Purpose and change:** No package or cross-project data reference was ultimately needed for seeding. Inventory never opens `catalogdb`, preserving the service boundary.

**Meaningful lines:** Temporary shared-source links were removed after exposing a nested test-build incompatibility; Inventory instead owns its small initializer below.

### FILE: `src/Inventory.API/Extensions/Extensions.cs`

**Before:** DI only registered `InventoryContext`.

**After:** `AddScoped<InventoryContextSeed>()` makes one seeder available per initialization scope. `AddHostedService<InventoryDatabaseInitializer>()` starts the local migration/seed worker. It depends on the context and new seeder.

### FILE: `src/Inventory.API/Infrastructure/InventoryContextSeed.cs`

**Why it exists:** A new Inventory database needs repeatable locations and one balance for every Catalog SKU at every supported distribution center.

**Sequential code explanation:** The primary constructor receives a logger. The fixed tuple array defines NCR, BLR, BOM, and HYD in stable order. `SeedAsync` checks each location by natural key before inserting, then saves parents before balances. Catalog's checked-in seed currently defines the contiguous IDs 1–101, so `Enumerable.Range(1, 101)` represents that approved baseline without making a runtime database call across service boundaries. Existing composite keys are loaded into a hash set. For each missing `(sku, location)`, the code derives `safetyStock = 10 + sku % 5`, `reorderPoint = safety + 20`, `onHand = 60 + sku % 41 + locationIndex * 10`, and `maxStock` at least 50 above both on-hand and reorder. Reserved begins at zero and the entity supplies version 1 plus UTC `UpdatedAt`. A final save persists only missing rows and a structured log reports the result.

**Dependencies/runtime flow:** startup initializer → `InventoryContextSeed` → locations → balances. Database uniqueness plus existence checks make reruns idempotent.

**Failure modes:** If Catalog adds a new checked-in seed ID, this explicit baseline must be updated in the same change. Existing rows are preserved, so seeding never resets live stock. Constructor and database checks reject any formula that violates stock policy.

### CHECKPOINT RESULT

Pending verification after the code change.

### Problem encountered

The first build failed because the shared migration helper calls `Activity.SetExceptionTags`, whose implementation lives in the separate shared `ActivityExtensions.cs` file. Linking that companion file fixed both compiler errors. This teaches that linked source files do not automatically bring sibling source files with them.

An initial attempt linked Catalog's JSON as external content. The MSTest SDK could build Inventory directly but failed while evaluating it as a project reference. Replacing the external content item with the reviewed contiguous SKU baseline (1–101) removed that build-graph incompatibility and kept the runtime service boundary explicit.

The nested-build failure remained while Inventory linked the shared migration source. `InventoryDatabaseInitializer.cs` therefore owns this small concern locally: its primary constructor receives the root service provider, environment, and logger; `ExecuteAsync` exits under the explicit `Testing` environment, creates an async DI scope, resolves the context and seeder, applies migrations, and seeds. A critical log plus rethrow makes startup failure visible. This avoids the linked-source issue while retaining migrate-then-seed behavior.

The .NET 10 MSTest project-reference negotiation also returned a silent failure in this environment. `tests/Inventory.UnitTests/Inventory.UnitTests.csproj` now marks its single-target Inventory reference with `SkipGetTargetFrameworkProperties="true"`; both projects already declare `net10.0`, so no framework selection is lost. The following test attempt reached the test platform but the workspace sandbox denied its local IPC named pipe. Running the identical test command with permitted local IPC completed successfully.

**Verification:** `dotnet build src/Inventory.API/Inventory.API.csproj --no-restore` succeeded with zero warnings/errors. `dotnet test tests/Inventory.UnitTests/Inventory.UnitTests.csproj --no-restore` passed all 18 tests. **Checkpoint: PASS.**

## Step 7 — Read inventory by SKU and location

------------------------------------------------------------
FILE: `src/Inventory.API/Application/InventoryContracts.cs`
------------------------------------------------------------

**Why this file exists:** HTTP endpoints, RabbitMQ handlers, and the business service need small messages they can share without exposing EF entities directly.

**Responsibility:** It defines the input/output boundary for reads, reservations, completion, restocking, and operation outcomes.

**Depends on:** Only .NET primitive types. **Depended on by:** `InventoryService`, `InventoryApi`, event processing, and tests.

**Code added:**

```csharp
public sealed record InventoryItemRequest(int SkuId, int Quantity);
public sealed record ReserveInventoryRequest(Guid OperationId, int OrderId, string LocationCode, IReadOnlyCollection<InventoryItemRequest> Items);
public sealed record CompleteInventoryRequest(Guid OperationId, int OrderId, string LocationCode);
public sealed record RestockInventoryRequest(Guid OperationId, int SkuId, string LocationCode, int Quantity, string? Reason);

public sealed record InventoryBalanceResponse(
    int SkuId, string LocationCode, int OnHand, int Reserved, int Available,
    int SafetyStock, int ReorderPoint, int MaxStock, long Version, DateTime UpdatedAt);

public enum InventoryOperationOutcome
{
    Succeeded,
    AlreadyProcessed,
    Invalid,
    NotFound,
    InsufficientStock,
    Conflict
}

public sealed record InventoryOperationResult(
    InventoryOperationOutcome Outcome,
    string Message,
    IReadOnlyCollection<int>? UnavailableSkuIds = null)
{
    public bool IsSuccess => Outcome is InventoryOperationOutcome.Succeeded or InventoryOperationOutcome.AlreadyProcessed;
}
```

**Sequential explanation:** `InventoryItemRequest` is one SKU and requested quantity. `ReserveInventoryRequest` carries a unique operation/message ID, the order, the chosen distribution center, and every line that must succeed together. `CompleteInventoryRequest` identifies an existing order reservation for commit or release. `RestockInventoryRequest` describes an independently idempotent delivery. The response returns stored values plus calculated `Available`. The enum avoids fragile string comparisons. `IsSuccess` deliberately treats a previously completed duplicate as success because callers may safely retry.

------------------------------------------------------------
FILE: `src/Inventory.API/Application/IInventoryService.cs`
------------------------------------------------------------

**Why/responsibility:** This interface keeps transport code separate from business/database code and makes the API testable with a fake service.

```csharp
public interface IInventoryService
{
    Task<InventoryBalanceResponse?> GetAsync(int skuId, string locationCode, CancellationToken cancellationToken);
    Task<InventoryOperationResult> ReserveAsync(ReserveInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> CommitAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> ReleaseAsync(CompleteInventoryRequest request, CancellationToken cancellationToken = default);
    Task<InventoryOperationResult> RestockAsync(RestockInventoryRequest request, CancellationToken cancellationToken = default);
}
```

Each line is one application use case. The nullable read result means “that SKU/location identity does not exist.”

------------------------------------------------------------
FILE: `src/Inventory.API/Application/InventoryService.cs`
------------------------------------------------------------

**Why it exists:** This is the single place that applies inventory rules and coordinates PostgreSQL transactions.

**Read code added first:**

```csharp
var normalized = Normalize(locationCode);
return await context.InventoryBalances
    .AsNoTracking()
    .Where(balance => balance.SkuId == skuId && balance.LocationCode == normalized)
    .Select(balance => new InventoryBalanceResponse(
        balance.SkuId, balance.LocationCode, balance.OnHand, balance.Reserved,
        balance.OnHand - balance.Reserved, balance.SafetyStock,
        balance.ReorderPoint, balance.MaxStock, balance.Version, balance.UpdatedAt))
    .SingleOrDefaultAsync(cancellationToken);
```

`Normalize` trims and uppercases location input. `AsNoTracking` avoids change-tracking overhead for a read. `Where` uses the real composite business identity. `Select` shapes a safe response and calculates available stock as `on_hand - reserved`. `SingleOrDefaultAsync` returns one row or `null` and propagates cancellation.

------------------------------------------------------------
FILE: `src/Inventory.API/Apis/InventoryApi.cs`
------------------------------------------------------------

**Why/responsibility:** This is the HTTP adapter. It validates transport input, calls `IInventoryService`, and converts domain outcomes into HTTP statuses.

```csharp
var versionedApi = app.NewVersionedApi("Inventory");
var api = versionedApi.MapGroup("api/inventory").HasApiVersion(1.0);

api.MapGet("/{skuId:int}/{locationCode}", GetInventoryAsync);
api.MapPost("/reservations", ReserveAsync);
api.MapPost("/reservations/commit", CommitAsync);
api.MapPost("/reservations/release", ReleaseAsync);
api.MapPost("/restocks", RestockAsync);
```

The first two lines create the v1 route group. The five following lines expose read, reserve, sale, release, and restock operations.

```csharp
if (skuId <= 0 || string.IsNullOrWhiteSpace(locationCode) || locationCode.Trim().Length > 16)
{
    return TypedResults.BadRequest("A positive SKU ID and a location code of at most 16 characters are required.");
}

var balance = await service.GetAsync(skuId, locationCode, cancellationToken);
return balance is null ? TypedResults.NotFound() : TypedResults.Ok(balance);
```

Invalid route values return 400, a valid but missing identity returns 404, and a found balance returns 200.

------------------------------------------------------------
FILE: `src/Inventory.API/Program.cs`
------------------------------------------------------------

**Changed code:**

```csharp
options.ReportApiVersions = true;
options.DefaultApiVersion = new ApiVersion(1, 0);
options.AssumeDefaultVersionWhenUnspecified = true;
// ...
app.MapInventoryApi();
```

The first three lines make v1 the default while reporting supported versions. The final line adds the new endpoints to ASP.NET routing.

------------------------------------------------------------
FILE: `src/Inventory.API/GlobalUsings.cs`
------------------------------------------------------------

Application, API, integration-event, EventBus, EF, and versioning namespaces were added as global usings. This keeps the small service files readable; it does not add runtime behavior.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Apis/InventoryApiTests.cs`
------------------------------------------------------------

**Why:** The tests run the real minimal-API pipeline on `TestServer` while replacing only `IInventoryService`.

**Meaningful sequence:** A `WebApplication` registers API versioning and a fake service, maps `InventoryApi`, and creates an in-memory HTTP client. Tests prove a known lowercase location becomes a 200 response, an unknown identity is 404, invalid input is 400, and insufficient reserve maps to 409 Conflict. This proves transport mapping without pretending an in-memory provider proves PostgreSQL concurrency.

### Step 7 checkpoint

`Inventory.API` built cleanly and its endpoint tests passed. Live verification later in this document returned SKU 42/NCR with `available = onHand - reserved`.

## Step 8 — Atomic reservation, idempotency, and concurrency

------------------------------------------------------------
FILE: `src/Inventory.API/Application/InventoryService.cs` (continued)
------------------------------------------------------------

**Reservation code and line-by-line flow:**

```csharp
var items = request.Items
    .GroupBy(item => item.SkuId)
    .Select(group => new InventoryItemRequest(group.Key, group.Sum(item => item.Quantity)))
    .OrderBy(item => item.SkuId)
    .ToArray();
```

Repeated SKU lines are combined, then sorted. Combining prevents two reservation rows for the same composite key. Stable sorting makes concurrent transactions lock balances in the same order and reduces deadlock risk.

```csharp
await using var transaction = await BeginTransactionIfNeededAsync(cancellationToken);
if (await IsDuplicateAsync(request.OperationId, InventoryMovementType.Reserve, cancellationToken))
{
    await RollbackIfOwnedAsync(transaction, cancellationToken);
    return new(InventoryOperationOutcome.AlreadyProcessed, "Reservation operation was already processed.");
}
```

The service opens a serializable transaction only if a caller has not already opened one. The movement ledger's source-event identity catches an exact retry before mutation.

```csharp
var existingReservations = await context.InventoryReservations
    .Where(reservation => reservation.OrderId == request.OrderId && reservation.LocationCode == location)
    .OrderBy(reservation => reservation.SkuId)
    .ToListAsync(cancellationToken);
if (existingReservations.Count > 0)
{
    await RollbackIfOwnedAsync(transaction, cancellationToken);
    var matchesExistingReservation = existingReservations.Count == items.Length &&
        existingReservations.Zip(items).All(pair =>
            pair.First.SkuId == pair.Second.SkuId && pair.First.Quantity == pair.Second.Quantity);
    return matchesExistingReservation
        ? new(InventoryOperationOutcome.AlreadyProcessed, "This order is already reserved at this location.")
        : new(InventoryOperationOutcome.Conflict, "This order already has a different reservation at this location.");
}
```

This second idempotency layer uses the business identity `(order, location)`. A broker redelivery with a new message ID and identical lines is still harmless. A different payload for an already-reserved order is a conflict instead of silently changing business facts. This final-audit addition also prevents a unique-key exception from poisoning an ambient inbox transaction.

```csharp
SELECT * FROM inventory.inventory_balances
WHERE location_code = {locationCode} AND sku_id = ANY ({skuIds})
ORDER BY sku_id FOR UPDATE
```

PostgreSQL locks all requested balance rows. The code then builds a dictionary, rejects any missing SKU, calculates every unavailable SKU, and returns before modifying anything if even one line cannot be satisfied. This is the all-or-nothing rule.

```csharp
foreach (var item in items)
{
    var balance = found[item.SkuId];
    balance.Reserve(item.Quantity, now);
    context.InventoryReservations.Add(new InventoryReservation(request.OrderId, item.SkuId, location, item.Quantity, now));
    AddMovement(request.OperationId, balance, request.OrderId, InventoryMovementType.Reserve, item.Quantity, now, "Order reservation");
}

await context.SaveChangesAsync(cancellationToken);
await CommitIfOwnedAsync(transaction, cancellationToken);
```

Only after every validation succeeds does each balance change, reservation row get added, and audit movement get recorded. One `SaveChanges` and one transaction commit make the set atomic.

**Optimistic plus pessimistic concurrency:** `FOR UPDATE` serializes access to the selected balances. Every entity operation increments `Version`; EF marks that property as a concurrency token, so generated updates include `WHERE version = oldVersion`. A zero-row update becomes `DbUpdateConcurrencyException`. PostgreSQL serialization failures and EF concurrency exceptions retry at most three times. The retry is wrapped in the Npgsql execution strategy when the service owns the transaction. This last detail was fixed after a live Aspire-style connection correctly rejected a user transaction outside its execution strategy.

**Failure modes:** invalid input returns `Invalid`; missing identities return `NotFound`; any short line returns `InsufficientStock`; a mismatched business redelivery returns `Conflict`; repeated concurrency failure returns `Conflict`; no partial reservation is committed.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Application/InventoryServicePostgresTests.cs`
------------------------------------------------------------

**Why:** These are real PostgreSQL tests because EF's in-memory provider cannot prove row locks, serializable transactions, database constraints, or actual optimistic concurrency SQL.

The first test resets/migrates a disposable database, attempts `[SKU1=2, SKU2=99]`, proves SKU1 stayed unchanged, then successfully reserves `[2,3]`. It retries both the same operation ID and a new message ID for the same order, proving both are harmless; it also proves a changed redelivery conflicts. Two independent contexts then concurrently request seven units from ten: exactly one succeeds, one reports insufficient stock, and the final reserved total is nine including the earlier two—not fourteen. BLR remains untouched, proving location isolation.

`INVENTORY_TEST_CONNECTION` is required. Without it the PostgreSQL tests explicitly report inconclusive instead of silently substituting a weaker database.

### Step 8 checkpoint

Real PostgreSQL concurrency and idempotency tests passed. The final suite contains 30/30 passing tests with zero skipped.

## Step 9 — Reservation HTTP API

------------------------------------------------------------
FILE: `src/Inventory.API/Apis/InventoryApi.cs` (reservation portion)
------------------------------------------------------------

```csharp
private static Task<IResult> ReserveAsync(
    ReserveInventoryRequest request,
    IInventoryService service,
    CancellationToken cancellationToken) =>
    ToHttpResultAsync(service.ReserveAsync(request, cancellationToken));

private static async Task<IResult> ToHttpResultAsync(Task<InventoryOperationResult> operation)
{
    var result = await operation;
    return result.Outcome switch
    {
        InventoryOperationOutcome.Succeeded => TypedResults.Ok(result),
        InventoryOperationOutcome.AlreadyProcessed => TypedResults.Ok(result),
        InventoryOperationOutcome.Invalid => TypedResults.BadRequest(result),
        InventoryOperationOutcome.NotFound => TypedResults.NotFound(result),
        InventoryOperationOutcome.InsufficientStock => TypedResults.Conflict(result),
        _ => TypedResults.Conflict(result)
    };
}
```

ASP.NET binds JSON to the request record and injects the service. Successful retries return 200 because the desired effect already exists. Bad input is 400, missing stock identity is 404, and business/concurrency conflicts are 409.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Inventory.UnitTests.csproj`
------------------------------------------------------------

TestHost and logging abstractions were added for HTTP tests. Direct references to EventBus and IntegrationEventLogEF ensure their runtime assemblies are copied. `SkipGetTargetFrameworkProperties="true"` works around this environment's .NET 10 nested project-framework negotiation issue; every involved project explicitly targets `net10.0`. `NoWarn` suppresses MSTEST0049 for the chosen project-wide MSTest style.

### Step 9 checkpoint

API tests verified success, normalized reads, 404, validation, and 409 mappings. Live curl later confirmed reserve and duplicate responses.

## Step 10 — Release, sale commit, and restock

------------------------------------------------------------
FILE: `src/Inventory.API/Model/InventoryBalance.cs`
------------------------------------------------------------

**Code added:**

```csharp
public int Available => OnHand - Reserved;

public void Reserve(int quantity, DateTime changedAt)
{
    RequirePositive(quantity);
    if (quantity > Available) throw new InvalidOperationException("Insufficient available inventory.");
    Reserved += quantity;
    Touch(changedAt);
}

public void CommitSale(int quantity, DateTime changedAt)
{
    RequirePositive(quantity);
    if (quantity > Reserved) throw new InvalidOperationException("Cannot sell more than reserved inventory.");
    Reserved -= quantity;
    OnHand -= quantity;
    Touch(changedAt);
}

public void Release(int quantity, DateTime changedAt)
{
    RequirePositive(quantity);
    if (quantity > Reserved) throw new InvalidOperationException("Cannot release more than reserved inventory.");
    Reserved -= quantity;
    Touch(changedAt);
}

public int Restock(int quantity, DateTime changedAt)
{
    RequirePositive(quantity);
    var accepted = Math.Min(quantity, MaxStock - OnHand);
    OnHand += accepted;
    if (accepted > 0) Touch(changedAt);
    return accepted;
}
```

Reserve changes only `reserved`. Commit changes both `reserved` and `on_hand`, which keeps available stock stable between reservation and payment. Release gives availability back without changing physical stock. Restock accepts only capacity up to `max_stock`. `Touch` requires UTC, increments `Version`, and updates `UpdatedAt`.

------------------------------------------------------------
FILE: `src/Inventory.API/Application/InventoryService.cs` (lifecycle portion)
------------------------------------------------------------

`CompleteOnceAsync` loads every reservation for `(order, location)`. No rows means `NotFound`; every row already in the requested terminal state means idempotent success; any other terminal state means `Conflict`. It locks balances in SKU order and either calls `CommitSale` + `reservation.Commit`, or `Release` + `reservation.Release`, then writes a movement for every line in the same transaction. `RestockOnceAsync` validates, checks the source operation ID, locks one balance, caps accepted quantity, records the actual accepted movement, and returns a clear capped message when appropriate.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Model/InventoryBalanceTests.cs`
------------------------------------------------------------

Five tests were added to prove reserve math, oversell rejection, sale math, release math, and max-capped restock/version changes.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Application/InventoryServicePostgresTests.cs` (lifecycle test)
------------------------------------------------------------

The test reserves and commits order 301, retries commit, reserves and releases order 302, and restocks SKU1 beyond capacity. It verifies final on-hand/reserved values and exactly five movements, proving duplicates did not add ledger rows.

### Step 10 checkpoint

Domain and PostgreSQL lifecycle tests passed, and the live API commit changed SKU42 from on-hand 61/reserved 2/version 2 to on-hand 59/reserved 0/version 3.

## Step 11 — Inbox and repository-native outbox

------------------------------------------------------------
FILE: `src/Inventory.API/Model/IncomingIntegrationEvent.cs`
------------------------------------------------------------

**Why:** RabbitMQ delivery is at-least-once, so Inventory needs a durable receipt keyed by event ID.

The constructor rejects an empty GUID, blank event type, or non-UTC timestamp. `EventId` is the immutable primary key; `EventType` and `ProcessedAt` make the receipt diagnosable.

------------------------------------------------------------
FILE: `src/Inventory.API/Infrastructure/EntityConfigurations/IncomingIntegrationEventEntityTypeConfiguration.cs`
------------------------------------------------------------

```csharp
builder.ToTable("incoming_integration_events");
builder.HasKey(message => message.EventId);
builder.Property(message => message.EventId).HasColumnName("event_id").ValueGeneratedNever();
builder.Property(message => message.EventType).HasColumnName("event_type").HasMaxLength(200).IsRequired();
builder.Property(message => message.ProcessedAt).HasColumnName("processed_at").IsRequired();
```

The database cannot create a second receipt with the same ID. The other lines give explicit PostgreSQL column names and limits.

------------------------------------------------------------
FILE: `src/Inventory.API/Model/InventoryShadowCheck.cs`
------------------------------------------------------------

**Why:** During migration, Inventory must calculate what it would decide without mutating stock, then compare that with Catalog's decision.

The entity stores source event, order, normalized location, Inventory decision/details/time, optional Catalog decision/time, and calculated `Agreement`. `ObserveCatalogResult` completes the comparison when Catalog's response arrives.

------------------------------------------------------------
FILE: `src/Inventory.API/Infrastructure/EntityConfigurations/InventoryShadowCheckEntityTypeConfiguration.cs`
------------------------------------------------------------

It maps `inventory_shadow_checks`, uses source-event ID as the primary key, limits location/details, ignores the calculated `Agreement` property, and indexes `order_id` so the later Catalog response can find its evaluation.

------------------------------------------------------------
FILE: `src/Inventory.API/Infrastructure/InventoryContext.cs`
------------------------------------------------------------

**Changed code:**

```csharp
public DbSet<IncomingIntegrationEvent> IncomingIntegrationEvents => Set<IncomingIntegrationEvent>();
public DbSet<InventoryShadowCheck> InventoryShadowChecks => Set<InventoryShadowCheck>();
// ...
modelBuilder.ApplyConfiguration(new IncomingIntegrationEventEntityTypeConfiguration());
modelBuilder.ApplyConfiguration(new InventoryShadowCheckEntityTypeConfiguration());
modelBuilder.UseIntegrationEventLogs();
```

The first two lines expose inbox and shadow tables. The configuration lines apply their mappings. `UseIntegrationEventLogs` reuses eShop's existing IntegrationEventLogEF outbox table in the Inventory schema.

------------------------------------------------------------
FILE: `src/Inventory.API/Infrastructure/Migrations/20260910074741_InventoryReliability.cs`
------------------------------------------------------------

The generated `Up` method creates `incoming_integration_events(event_id PK, event_type, processed_at)`, repository-native `IntegrationEventLog(EventId PK, EventTypeName, State, TimesSent, CreationTime, Content, TransactionId)`, and `inventory_shadow_checks` plus its order index. `Down` drops those three tables in reverse-safe order.

------------------------------------------------------------
FILES: `src/Inventory.API/Infrastructure/Migrations/20260910074741_InventoryReliability.Designer.cs` and `InventoryContextModelSnapshot.cs`
------------------------------------------------------------

These generated EF files describe the exact post-migration model used by tooling. They were not hand-edited. The meaningful additions mirror the three tables above. Keeping the snapshot lets the next migration calculate only future differences.

------------------------------------------------------------
FILE: `src/Inventory.API/Infrastructure/InventoryContextDesignFactory.cs`
------------------------------------------------------------

**Why:** EF migration tooling must construct `InventoryContext` without starting the web host, RabbitMQ connection, or initializer.

It reads `ConnectionStrings__inventorydb`, falls back to a design-only local string, configures `UseNpgsql`, and returns a context. No connection is opened just to inspect or scaffold the model.

### Step 11 checkpoint

The migration script was generated and inspected. The PostgreSQL test proves business changes, inbox receipt, and outbox entry commit together, while an exact duplicate produces no second effect.

## Step 12 — RabbitMQ handlers and publishing

------------------------------------------------------------
FILE: `src/Inventory.API/IntegrationEvents/Events/OrderLifecycleIntegrationEvents.cs`
------------------------------------------------------------

The seven short records intentionally use the same simple type names and JSON shapes as Ordering/Catalog contracts: stock line, confirmed line, AwaitingValidation with `OrderId + LocationCode + items`, Paid/Cancelled with `OrderId + LocationCode`, and confirmed/rejected outputs. eShop's event bus routes by event type name, and extra JSON properties are safely ignored.

------------------------------------------------------------
FILE: `src/Inventory.API/IntegrationEvents/EventHandling/OrderLifecycleIntegrationEventHandlers.cs`
------------------------------------------------------------

Five handlers were created. Each has one dependency, `InventoryIntegrationEventProcessor`, and one line of behavior: Awaiting, Paid, and Cancelled call their matching `ProcessAsync`; Catalog's confirmed/rejected results call `ObserveCatalogAsync`. Thin adapters keep transaction/idempotency logic centralized.

------------------------------------------------------------
FILE: `src/Inventory.API/IntegrationEvents/InventoryIntegrationEventService.cs`
------------------------------------------------------------

```csharp
await eventLog.MarkEventAsInProgressAsync(integrationEvent.Id);
await eventBus.PublishAsync(integrationEvent);
await eventLog.MarkEventAsPublishedAsync(integrationEvent.Id);
```

The outbox row is first marked in progress, then RabbitMQ is called, then success is persisted. An exception is logged and the row is marked failed, leaving a visible/retryable record rather than losing the fact that publication failed.

------------------------------------------------------------
FILE: `src/Inventory.API/IntegrationEvents/InventoryIntegrationEventProcessor.cs`
------------------------------------------------------------

**Authoritative Awaiting flow:** It maps order lines to `ReserveInventoryRequest`, using the incoming event ID as the operation ID. Success/duplicate creates `OrderStockConfirmedIntegrationEvent`; every failure creates `OrderStockRejectedIntegrationEvent` and marks unavailable lines.

**Paid/Cancelled flow:** Outside shadow mode, Paid calls `CommitAsync` and Cancelled calls `ReleaseAsync`. Neither publishes a stock-validation response.

**Atomic inbox/outbox flow:**

```csharp
var strategy = context.Database.CreateExecutionStrategy();
var outgoing = await strategy.ExecuteAsync(async () =>
{
    await using var transaction = await context.Database.BeginTransactionAsync(IsolationLevel.Serializable);
    if (await context.IncomingIntegrationEvents.AnyAsync(message => message.EventId == incoming.Id))
    {
        await transaction.RollbackAsync();
        return null;
    }

    var (_, eventToPublish) = await operation();
    context.IncomingIntegrationEvents.Add(new IncomingIntegrationEvent(incoming.Id, incoming.GetType().Name, DateTime.UtcNow));
    if (eventToPublish is not null)
    {
        context.Set<IntegrationEventLogEntry>().Add(new IntegrationEventLogEntry(eventToPublish, transaction.TransactionId));
    }

    await context.SaveChangesAsync();
    await transaction.CommitAsync();
    return eventToPublish;
});
```

The retrying execution strategy owns the entire serializable unit. A known event ID exits without business work. Otherwise the business mutation, inbox receipt, and optional outbox row share one context and transaction. Publication occurs only after commit, so RabbitMQ cannot announce stock confirmation for rolled-back stock.

**Failure mode:** A crash before commit leaves nothing and RabbitMQ redelivers. A crash after commit is recognized by the inbox; the durable outbox row still records publication state. This repository's outbox publisher attempts immediately and records failure; a continuously polling outbox dispatcher is a known hardening item noted later.

------------------------------------------------------------
FILE: `src/Inventory.API/Extensions/Extensions.cs`
------------------------------------------------------------

DI registrations added `IInventoryService`, repository `IIntegrationEventLogService`, publisher, scoped processor, and bound `InventoryOptions`. `AddRabbitMqEventBus("eventbus")` subscribes all five handlers listed above.

------------------------------------------------------------
FILE: `src/Inventory.API/Inventory.API.csproj`
------------------------------------------------------------

Project references were added for `EventBus`, `EventBusRabbitMQ`, and `IntegrationEventLogEF`. This reuses the application's native integration mechanism rather than introducing Kafka into operational commands. EF Tools remains private to build/tooling consumers.

------------------------------------------------------------
FILE: `src/Inventory.API/appsettings.json`
------------------------------------------------------------

```json
"Inventory": { "ShadowMode": false },
"EventBus": { "SubscriptionClientName": "Inventory", "RetryCount": 5 }
```

The final configuration makes Inventory authoritative. Its Rabbit subscription has an independent queue name and five connection retries.

------------------------------------------------------------
FILE: `src/Inventory.API/InventoryOptions.cs`
------------------------------------------------------------

`SectionName = "Inventory"` prevents repeated string literals; writable `ShadowMode` allows options binding.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Application/InventoryServicePostgresTests.cs` (event tests)
------------------------------------------------------------

The inbox/outbox test delivers one Awaiting event twice and proves reserved=3, one inbox row, one reserve movement, one outbox row, and one published confirmation. The shadow test proves evaluation/comparison persists while reservation and publication remain zero. The authoritative lifecycle test delivers duplicate Paid and Cancelled messages and proves one physical sale, one release, two reserve movements, and only the expected two confirmations.

### Step 12 checkpoint

RabbitMQ wiring built cleanly. PostgreSQL-backed event/idempotency tests passed. A live RabbitMQ connection was established during the final smoke test.

## Step 13 — AppHost resources

------------------------------------------------------------
FILE: `src/eShop.AppHost/Program.cs`
------------------------------------------------------------

```csharp
var inventoryDb = postgres.AddDatabase("inventorydb");

var inventoryApi = builder.AddProject<Projects.Inventory_API>("inventory-api")
    .WithReference(rabbitMq).WaitFor(rabbitMq)
    .WithReference(inventoryDb).WaitFor(inventoryDb)
    .WithHttpHealthCheck("/health");
```

The first line gives Inventory its own logical PostgreSQL database. The project resource receives RabbitMQ and database connection strings, waits for both dependencies, and exposes `/health` to Aspire. Catalog and Ordering databases remain separate.

------------------------------------------------------------
FILE: `src/eShop.AppHost/eShop.AppHost.csproj`
------------------------------------------------------------

The Inventory project reference generates the typed `Projects.Inventory_API` symbol used above. `ASPIRE010` is suppressed for AppHost because this repository intentionally has `AspireUseCliBundle=false`; it does not hide compiler or application warnings.

### Step 13 checkpoint

The AppHost build traversed the full distributed project graph and succeeded with 0 warnings and 0 errors. AppHost's 7 tests also passed.

## Step 14 — Carry LocationCode from checkout to Inventory

------------------------------------------------------------
FILE: `src/WebApp/Services/BasketCheckoutInfo.cs`
------------------------------------------------------------

**Why:** The shopper's distribution-center choice must exist before an order is submitted.

```csharp
[Required]
public string LocationCode { get; set; } = "NCR";
```

`Required` participates in Blazor form validation. The NCR default preserves compatibility for old clients and ordinary demo checkout.

------------------------------------------------------------
FILE: `src/WebApp/Components/Pages/Checkout/Checkout.razor`
------------------------------------------------------------

```razor
<InputSelect @bind-Value="@Info.LocationCode">
    <option value="NCR">NCR — National Capital Region</option>
    <option value="BLR">BLR — Bengaluru</option>
    <option value="BOM">BOM — Mumbai</option>
    <option value="HYD">HYD — Hyderabad</option>
</InputSelect>
<ValidationMessage For="@(() => Info.LocationCode)" />
```

The select limits normal UI input to the four seeded centers. Two-way binding writes the choice into checkout state; the validation component displays any model error.

------------------------------------------------------------
FILE: `src/WebApp/Services/BasketState.cs`
------------------------------------------------------------

**Before:** `CreateOrderRequest` ended at `Items`.

**After:** Checkout passes `LocationCode: checkoutInfo.LocationCode`, and the local request contract adds `string LocationCode = "NCR"` as its final field. Being last and optional avoids breaking existing positional callers.

------------------------------------------------------------
FILE: `src/Ordering.API/Apis/OrdersApi.cs`
------------------------------------------------------------

The API-side `CreateOrderRequest` received the same final optional field. `CreateOrderCommand` is now called with `request.LocationCode`, preserving the value across the HTTP boundary.

------------------------------------------------------------
FILE: `src/Ordering.API/Application/Commands/CreateOrderCommand.cs`
------------------------------------------------------------

```csharp
[DataMember]
public string LocationCode { get; private set; }
// constructor final parameter:
string locationCode = "NCR"
// constructor body:
LocationCode = locationCode;
```

The serialized command now carries the location. The default protects existing tests/callers. Validation and the aggregate—not this transport object—normalize and enforce the value.

------------------------------------------------------------
FILE: `src/Ordering.API/Application/Validations/CreateOrderCommandValidator.cs`
------------------------------------------------------------

```csharp
RuleFor(command => command.LocationCode)
    .Must(code => code?.Trim().ToUpperInvariant() is "NCR" or "BLR" or "BOM" or "HYD")
    .WithMessage("LocationCode must be NCR, BLR, BOM, or HYD");
```

The command boundary rejects unsupported sites early while accepting harmless case/space differences.

------------------------------------------------------------
FILE: `src/Ordering.API/Application/Commands/CreateOrderCommandHandler.cs`
------------------------------------------------------------

The `Order` constructor call gained `locationCode: message.LocationCode`. A named argument makes the addition unambiguous after existing optional buyer/payment arguments.

------------------------------------------------------------
FILE: `src/Ordering.Domain/AggregatesModel/OrderAggregate/Order.cs`
------------------------------------------------------------

**Why:** Location is a durable order fact. Payment and cancellation events must use the original selected center, not a current UI setting.

```csharp
public string LocationCode { get; private set; }
// ... constructor ...
LocationCode = NormalizeLocationCode(locationCode);
```

```csharp
private static string NormalizeLocationCode(string locationCode)
{
    if (string.IsNullOrWhiteSpace(locationCode))
        throw new OrderingDomainException("A distribution-center location is required.");

    var normalized = locationCode.Trim().ToUpperInvariant();
    if (normalized is not ("NCR" or "BLR" or "BOM" or "HYD"))
        throw new OrderingDomainException($"Unsupported inventory location '{normalized}'.");

    return normalized;
}
```

The aggregate itself enforces the invariant even if a caller bypasses HTTP validation. This is the last line of defense before persistence.

------------------------------------------------------------
FILE: `src/Ordering.Infrastructure/EntityConfigurations/OrderEntityTypeConfiguration.cs`
------------------------------------------------------------

```csharp
orderConfiguration.Property(o => o.LocationCode)
    .HasColumnName("LocationCode")
    .HasMaxLength(16)
    .HasDefaultValue("NCR")
    .IsRequired();
```

Ordering persists the field in `ordering.orders`. The database default backfills existing rows safely during migration.

------------------------------------------------------------
FILE: `src/Ordering.Infrastructure/Migrations/20260910075244_AddOrderLocationCode.cs`
------------------------------------------------------------

The generated `Up` adds required `character varying(16) LocationCode` with default NCR. `Down` drops precisely that column.

------------------------------------------------------------
FILES: `src/Ordering.Infrastructure/Migrations/20260910075244_AddOrderLocationCode.Designer.cs` and `OrderingContextModelSnapshot.cs`
------------------------------------------------------------

EF generated these model descriptions. The meaningful new model line is the required 16-character `LocationCode` with default NCR. EF 10 also refreshed how pre-existing backing-field properties are represented in metadata; inspection of the actual migration verified the executable change is only the new column.

------------------------------------------------------------
FILES: `src/Ordering.API/Application/IntegrationEvents/Events/OrderStatusChangedToAwaitingValidationIntegrationEvent.cs`, `OrderStatusChangedToPaidIntegrationEvent.cs`, and `OrderStatusChangedToCancelledIntegrationEvent.cs`
------------------------------------------------------------

Each contract gained:

```csharp
public string LocationCode { get; }
// constructor parameter: string locationCode
// constructor assignment:
LocationCode = locationCode;
```

Awaiting still carries line items for reservation. Paid still carries the existing extra line information, though Inventory commits from its stored reservation. Cancelled needs only order/location. The shared property name is what Inventory's same-name event records deserialize.

------------------------------------------------------------
FILES: `src/Ordering.API/Application/DomainEventHandlers/OrderStatusChangedToAwaitingValidationDomainEventHandler.cs`, `OrderStatusChangedToPaidDomainEventHandler.cs`, and `OrderCancelledDomainEventHandler.cs`
------------------------------------------------------------

Every integration-event constructor now receives `order.LocationCode`. These handlers reload the persisted order, so later lifecycle events cannot drift from the location selected at creation.

------------------------------------------------------------
FILES: `src/Ordering.API/Application/Queries/OrderViewModel.cs` and `OrderQueries.cs`
------------------------------------------------------------

Both detailed `Order` and list `OrderSummary` view models gained `LocationCode`. Both query projections assign `order.LocationCode`. This makes the persisted routing decision visible to clients and operators.

------------------------------------------------------------
FILE: `tests/Ordering.UnitTests/Domain/OrderAggregateTest.cs`
------------------------------------------------------------

The new test constructs an order with `" blr "`, verifies stored `BLR`, and verifies `UNKNOWN` throws `OrderingDomainException`. It directly proves aggregate normalization and rejection.

### Step 14 checkpoint

WebApp and Ordering built with zero warnings/errors. Ordering unit tests passed 44/44 and functional tests passed 11/11.

## Step 15 — Shadow mode

------------------------------------------------------------
FILES: `src/Inventory.API/InventoryOptions.cs`, `appsettings.json`, `Model/InventoryShadowCheck.cs`, `Infrastructure/EntityConfigurations/InventoryShadowCheckEntityTypeConfiguration.cs`, and `IntegrationEvents/InventoryIntegrationEventProcessor.cs`
------------------------------------------------------------

These files were introduced in Steps 11–12 and then used for the migration checkpoint. With `ShadowMode=true`, Awaiting events read every requested balance and record whether Inventory *would* confirm. They do not reserve and do not publish an order decision. Incoming Catalog confirmed/rejected events fill `CatalogConfirmed`, and `Agreement` shows whether both authorities would have made the same decision. Paid/Cancelled are acknowledged without mutation in shadow mode.

**Failure modes:** A missing shadow record leaves no comparison but does not affect the live Catalog decision. Duplicate event IDs are stopped by the inbox. Shadow is observability, never authority.

------------------------------------------------------------
FILE: `tests/Inventory.UnitTests/Application/InventoryServicePostgresTests.cs` (shadow test)
------------------------------------------------------------

The test evaluates and observes an order, then proves `InventoryConfirmed`, `CatalogConfirmed`, and `Agreement` are true while `Reserved` remains zero and the fake event bus publishes nothing.

### Step 15 checkpoint

PostgreSQL shadow test passed. After cutover, the final checked-in setting is intentionally `ShadowMode=false`.

## Step 16 — Inventory owns AwaitingValidation

------------------------------------------------------------
FILE: `src/Catalog.API/Extensions/Extensions.cs`
------------------------------------------------------------

**Before:** Catalog subscribed its stock-validation and paid-stock handlers.

**After:**

```csharp
// Inventory.API is now the single stock authority. Catalog retains the
// event bus only for catalog-owned events such as product price changes.
builder.AddRabbitMqEventBus("eventbus");
```

Removing Catalog's Awaiting subscription is the decisive authority cutover. Inventory alone responds with the existing `OrderStockConfirmed` or `OrderStockRejected` contract, so Ordering's state machine does not need a rewrite.

------------------------------------------------------------
FILE DELETED: `src/Catalog.API/IntegrationEvents/EventHandling/OrderStatusChangedToAwaitingValidationIntegrationEventHandler.cs`
------------------------------------------------------------

The removed handler read `CatalogItem.AvailableStock` and published a decision. Keeping it would allow two authorities to race and send contradictory responses, so deletion is required—not dead-code cleanup.

------------------------------------------------------------
FILE: `src/Catalog.API/GlobalUsings.cs`
------------------------------------------------------------

The now-unused Catalog event-handler namespace global using was removed, preventing a stale compile-time dependency.

### Step 16 checkpoint

Catalog and Inventory built, Inventory authoritative reservation tests passed, and Catalog functional tests remained green.

## Step 17 — Inventory owns Paid/Cancelled lifecycle

------------------------------------------------------------
FILE DELETED: `src/Catalog.API/IntegrationEvents/EventHandling/OrderStatusChangedToPaidIntegrationEventHandler.cs`
------------------------------------------------------------

The old code called `CatalogItem.RemoveStock`. It was deleted so a successful order can never make both Catalog and Inventory deduct physical stock.

------------------------------------------------------------
FILES: Ordering Paid/Cancelled integration-event contracts and domain handlers
------------------------------------------------------------

As documented in Step 14, both messages now include the persisted location. Inventory's Paid handler commits the matching reservation, reducing both on-hand and reserved exactly once. Inventory's Cancelled handler releases reserved quantity without changing on-hand exactly once.

------------------------------------------------------------
FILE: `src/Inventory.API/IntegrationEvents/InventoryIntegrationEventProcessor.cs`
------------------------------------------------------------

```csharp
var result = await inventory.CommitAsync(
    new(integrationEvent.Id, integrationEvent.OrderId, integrationEvent.LocationCode));
// or
var result = await inventory.ReleaseAsync(
    new(integrationEvent.Id, integrationEvent.OrderId, integrationEvent.LocationCode));
```

Using the event ID supplies message idempotency; stored `(order, location)` reservations supply business-state idempotency. The inbox wraps both paths.

### Step 17 checkpoint

The lifecycle integration test delivered Paid and Cancelled twice and observed one sale and one release. Catalog has no stock lifecycle subscription.

## Step 18 — Deprecate Catalog stock authority

------------------------------------------------------------
FILE: `src/Catalog.API/Model/CatalogItem.cs`
------------------------------------------------------------

**Removed code:** `AvailableStock`, `RestockThreshold`, `MaxStockThreshold`, `OnReorder`, `RemoveStock`, and `AddStock` were deleted.

**Why:** Leaving mutable stock fields on the Catalog model would invite future code to treat stale values as authoritative. Catalog now owns product identity, name, brand, type, description, price, picture, and embedding; Inventory owns quantities and policies by location.

------------------------------------------------------------
FILE: `src/Catalog.API/Infrastructure/CatalogContextSeed.cs`
------------------------------------------------------------

The seed assignments `AvailableStock = 100`, `MaxStockThreshold = 200`, and `RestockThreshold = 10` were removed. New Catalog products no longer create misleading global stock.

------------------------------------------------------------
FILE: `src/Catalog.API/Apis/CatalogApi.cs`
------------------------------------------------------------

Catalog item creation stopped copying the three stock inputs. The API still creates all catalog-owned product fields and its embedding.

------------------------------------------------------------
FILES: `src/Catalog.API/Catalog.API.json` and `Catalog.API_v2.json`
------------------------------------------------------------

The generated OpenAPI schemas removed `availableStock`, `restockThreshold`, `maxStockThreshold`, and `onReorder`. Clients can no longer mistake Catalog's API for an inventory API.

------------------------------------------------------------
FILE: `src/Catalog.API/Infrastructure/Migrations/20260910075536_RemoveCatalogStockOwnership.cs`
------------------------------------------------------------

`Up` drops the four Catalog columns. `Down` can restore them with safe type defaults if the migration itself must be rolled back. This is intentionally a destructive production-data migration: a real deployment should first retain a backup and complete/observe shadow reconciliation. It was generated and inspected here; no production database was altered.

------------------------------------------------------------
FILES: `src/Catalog.API/Infrastructure/Migrations/20260910075536_RemoveCatalogStockOwnership.Designer.cs` and `CatalogContextModelSnapshot.cs`
------------------------------------------------------------

EF generated the final Catalog model without the four stock properties. As with Ordering, EF 10 refreshed product-version/metadata formatting; the executable migration was inspected and contains only the four intended drops.

------------------------------------------------------------
FILE: `tests/Catalog.FunctionalTests/CatalogApiTests.cs`
------------------------------------------------------------

Two update tests formerly changed `AvailableStock`; they now change `Description` and still verify persistence (one also verifies price). The create fixture removed all four deleted properties. This preserves the tests' Catalog CRUD purpose without testing inventory behavior in the wrong service.

### Step 18 checkpoint

Catalog built with zero warnings/errors, functional tests passed 36/36, and EF reported no pending Catalog model changes. Catalog is no longer a stock authority in code, messaging, persistence model, seed, or public schema.

# Final Architecture

```text
WebApp checkout
  -> Ordering API / Order(LocationCode)
  -> RabbitMQ AwaitingValidation
  -> Inventory inbox + serializable reservation + outbox
  -> RabbitMQ StockConfirmed/Rejected
  -> Ordering state machine
  -> RabbitMQ Paid or Cancelled
  -> Inventory commit-sale or release
```

PostgreSQL remains database-per-service: `catalogdb`, `orderingdb`, and new `inventorydb`. RabbitMQ remains eShop's operational event bus. No Kafka, Debezium, Rust, agent, forecasting, supplier system, Playwright, or k6 work was introduced.

# Database Schema

Inventory uses schema `inventory` and two migrations: `InitialInventory`, then `InventoryReliability`. Ordering adds one location column. Catalog removes four obsolete global-stock columns.

# Table-by-Table Explanation

| Table | Identity | Purpose |
|---|---|---|
| `inventory.locations` | `code` | Four supported distribution centers. |
| `inventory.inventory_balances` | `(sku_id, location_code)` | Current physical, reserved, policy, version, and update time. |
| `inventory.inventory_reservations` | `(order_id, sku_id, location_code)` | Per-line reservation state machine. |
| `inventory.inventory_movements` | `movement_id` | Append-only operational audit/CDC fact with source idempotency index. |
| `inventory.incoming_integration_events` | `event_id` | Durable inbox receipt. |
| `inventory.IntegrationEventLog` | `EventId` | Repository-native outgoing event log/outbox. |
| `inventory.inventory_shadow_checks` | `source_event_id` | Pre-cutover comparison evidence. |
| `ordering.orders` | existing order ID | Now includes required `LocationCode`. |

Database check constraints enforce `0 <= reserved <= on_hand <= max_stock` and `0 <= safety_stock <= reorder_point <= max_stock`. `Available` is calculated, so it cannot drift as an independently stored value.

# Successful Order Flow

The shopper selects a location. Ordering persists it and publishes Awaiting with every line. Inventory's inbox transaction locks all balances, validates all lines, raises reserved amounts, inserts reservations/movements, stores the incoming receipt and confirmation outbox row, then commits. Ordering receives confirmation and proceeds. Paid later commits stored reservations: both on-hand and reserved fall by the reserved quantities. Catalog never deducts stock.

# Insufficient Stock Flow

Inventory locks and evaluates every line before changing any. If one is missing or short, the reservation transaction makes no balance/reservation/movement changes and publishes the compatible rejected result. Ordering cancels/rejects through its existing state machine. This is all-or-nothing across order lines.

# Payment Failure Flow

The order cancellation event carries the persisted location. Inventory finds all still-reserved lines, decreases `reserved`, marks reservations Released, and writes Release movements. `on_hand` does not change. Duplicate cancellation is a no-op success.

# Restock Flow

`POST /api/inventory/restocks` identifies an operation, SKU, location, quantity, and reason. The service locks the balance, accepts at most `max_stock - on_hand`, increments version/time, and records the actual accepted amount. Retrying the operation ID cannot add stock twice.

# Reservation State Machine

```text
                 Paid
Reserved -----------------> Committed
    |
    | Cancelled/payment failure
    v
Released
```

Committed and Released are terminal. Repeating the same terminal transition succeeds idempotently; attempting the opposite terminal transition conflicts.

# Integration Event Matrix

| Incoming event | Inventory action | Outgoing event |
|---|---|---|
| AwaitingValidation | Atomic reserve, or shadow evaluation | StockConfirmed/StockRejected when authoritative; none in shadow |
| Paid | Commit reservation to sale | None |
| Cancelled | Release reservation | None |
| Catalog StockConfirmed/Rejected | Complete comparison in shadow only | None |

# Idempotency Strategy

There are three layers: inbox primary key stops the same event delivery; the movement unique index stops the same operation/SKU/location/type; reservation business identity stops the same order/location even if a producer emits a new message ID. Terminal reservation state makes Paid/Cancelled repeat-safe. All idempotency records commit with their business effects.

# Concurrency Strategy

Requests normalize/group/sort SKUs, use PostgreSQL serializable transactions, and acquire `FOR UPDATE` locks in stable order. Balance `Version` is an EF concurrency token and is manually incremented on every mutation, producing `UPDATE ... WHERE version = old`. EF/Npgsql concurrency failures have a bounded retry path. These layers prevent lost updates and overselling.

# Shadow Mode Migration

Shadow mode computes and stores Inventory decisions while Catalog remains authoritative and Inventory makes no stock mutation. Catalog responses fill comparisons. After agreement is considered acceptable, set `ShadowMode=false`, enable Inventory authority, then remove Catalog subscriptions/fields. The checked-in final state is after that cutover.

# Catalog-to-Inventory Ownership Cutover

Awaiting authority moved first, then Paid/Cancelled ownership, then Catalog's model/schema/API fields were removed. The key safety property is now structural: only Inventory subscribes to stock lifecycle messages and only Inventory has stock mutation methods.

# CDC Readiness

`inventory_movements` is the best first CDC fact: it is append-only, contains SKU/location/order/type/quantity/source/timestamps, and is indexed by SKU/location/time and recorded time. `inventory_balances` supplies current-state snapshots. Future Debezium should read PostgreSQL WAL; analytics must not repeatedly query production tables and must not replace RabbitMQ.

# Every File Created

Steps 1–6 list the shell, model, configuration, initial migration, and seeding files earlier in this document. Steps 7–18 created the three application files, Inventory API adapter, PostgreSQL/API tests, inbox/shadow entities and configurations, design factory, reliability migration/designer, integration-event contracts/handlers/processor/publisher, options, Ordering migration/designer, and Catalog removal migration/designer. The exact paths appear at their chronological sections above.

# Every File Modified

The chronological sections explain `Program.cs`, `GlobalUsings.cs`, Inventory project/settings/context/extensions/domain/tests, WebApp checkout/state contracts, Ordering request/command/aggregate/configuration/events/queries/migration snapshot/test, AppHost program/project, Catalog API/model/seed/extensions/global usings/OpenAPI/migration snapshot/tests, and `eShop.slnx`. Generated snapshots/OpenAPI changed only to represent the new runtime contracts. `AGENTS.md` and `tmp/` are user-owned/unrelated and were not changed by this implementation run.

# Exact Build/Test Commands

```bash
dotnet build src/Inventory.API/Inventory.API.csproj --no-restore -m:1
dotnet build src/Ordering.API/Ordering.API.csproj --no-restore -m:1
dotnet build src/Catalog.API/Catalog.API.csproj --no-restore -m:1
dotnet build src/WebApp/WebApp.csproj --no-restore -m:1
dotnet build src/eShop.AppHost/eShop.AppHost.csproj --no-restore -m:1

INVENTORY_TEST_CONNECTION='Host=localhost;Port=55432;Database=inventorytests;Username=postgres;Password=postgres' \
dotnet test tests/Inventory.UnitTests/Inventory.UnitTests.csproj --no-restore -p:BuildInParallel=false

dotnet test tests/Ordering.UnitTests/Ordering.UnitTests.csproj --no-restore -p:BuildInParallel=false
dotnet test tests/Ordering.FunctionalTests/Ordering.FunctionalTests.csproj --no-restore -p:BuildInParallel=false
dotnet test tests/Catalog.FunctionalTests/Catalog.FunctionalTests.csproj --no-restore -p:BuildInParallel=false
dotnet test tests/Application.UnitTests/Application.UnitTests.csproj --no-restore -p:BuildInParallel=false
dotnet test tests/eShop.AppHost.UnitTests/eShop.AppHost.UnitTests.csproj --no-restore -p:BuildInParallel=false
```

`-m:1`/`BuildInParallel=false` were used because this local .NET 10 SDK silently fails nested parallel project negotiation for this graph; this is an environment/build-graph workaround, not business behavior.

# Final Verification Results

All five affected project builds completed with zero warnings and zero errors; the AppHost build also compiled every referenced service. Tests: Inventory 30/30, Ordering unit 44/44, Ordering functional 11/11, Catalog functional 36/36, shared Application unit 13/13, AppHost 7/7: **141 passed, 0 failed, 0 skipped**. Two functional projects emitted their existing `ASPIRE010` CLI-bundle warning. Inventory, Ordering, and Catalog each reported no pending EF model changes.

Live Inventory verification connected to disposable PostgreSQL and RabbitMQ, migrated/seeding successfully, returned `/health` 200, and reported 4 locations, 404 SKU/location balances, and 0 invariant violations. GET SKU42/NCR returned on-hand 61/reserved 0/available 61/version 1. A two-line reserve returned 200; the same order with a different message ID returned `AlreadyProcessed`; commit returned 200; final SKU42 was on-hand 59/reserved 0/available 59/version 3.

# Known Limitations

- The deterministic checked-in seed baseline is Catalog IDs 1–101; adding checked-in Catalog products requires extending this reviewed baseline or later creating balances through an explicit Inventory-owned API/event.
- The native outgoing log records failed publication, but this implementation attempts publication immediately; production hardening should add/verify a continuous retry dispatcher and operational alerting for failed rows.
- The full browser checkout/order lifecycle was not manually driven end-to-end in a browser. Cross-service builds, functional tests, real PostgreSQL tests, live Inventory HTTP, seed, database invariants, and RabbitMQ connection were verified.
- Catalog's column-drop migration is intentionally destructive. A production rollout needs backup, shadow reconciliation evidence, and staged deployment order before applying it.

# How To Run Locally

Preferred: run `dotnet run --project src/eShop.AppHost/eShop.AppHost.csproj`; Aspire now provisions `inventorydb`, passes its connection plus RabbitMQ, waits for dependencies, and monitors `/health`.

To run Inventory alone, provide `ConnectionStrings__inventorydb` and `ConnectionStrings__eventbus`, then run `dotnet run --project src/Inventory.API/Inventory.API.csproj`. Startup applies migrations and idempotently seeds balances outside the `Testing` environment.

# CURL/API Examples

```bash
curl -i http://localhost:5225/health
curl -i http://localhost:5225/api/inventory/42/NCR

curl -i -X POST http://localhost:5225/api/inventory/reservations \
  -H 'Content-Type: application/json' \
  --data '{"operationId":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","orderId":9001,"locationCode":"NCR","items":[{"skuId":42,"quantity":2},{"skuId":43,"quantity":3}]}'

curl -i -X POST http://localhost:5225/api/inventory/reservations/commit \
  -H 'Content-Type: application/json' \
  --data '{"operationId":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","orderId":9001,"locationCode":"NCR"}'

curl -i -X POST http://localhost:5225/api/inventory/reservations/release \
  -H 'Content-Type: application/json' \
  --data '{"operationId":"cccccccc-cccc-cccc-cccc-cccccccccccc","orderId":9002,"locationCode":"NCR"}'

curl -i -X POST http://localhost:5225/api/inventory/restocks \
  -H 'Content-Type: application/json' \
  --data '{"operationId":"dddddddd-dddd-dddd-dddd-dddddddddddd","skuId":42,"locationCode":"NCR","quantity":10,"reason":"supplier delivery"}'
```

# Useful PostgreSQL Queries

```sql
SELECT sku_id, location_code, on_hand, reserved,
       on_hand - reserved AS available, version
FROM inventory.inventory_balances
WHERE sku_id = 42 AND location_code = 'NCR';

SELECT * FROM inventory.inventory_reservations
WHERE order_id = 9001 ORDER BY sku_id;

SELECT movement_type, sku_id, location_code, quantity, order_id, occurred_at
FROM inventory.inventory_movements
ORDER BY recorded_at DESC LIMIT 50;

SELECT COUNT(*) AS invariant_violations
FROM inventory.inventory_balances
WHERE reserved < 0 OR reserved > on_hand OR on_hand > max_stock
   OR safety_stock < 0 OR safety_stock > reorder_point OR reorder_point > max_stock;

SELECT state, COUNT(*) FROM inventory."IntegrationEventLog" GROUP BY state;
SELECT inventory_confirmed, catalog_confirmed, agreement, COUNT(*)
FROM inventory.inventory_shadow_checks GROUP BY 1, 2, 3;
```

# What Happens Next

The approved next direction is **Debezium → Kafka → streaming feature engine**. That future analytics/CDC path should consume WAL changes, starting with the movement ledger, while RabbitMQ continues carrying operational application events.
