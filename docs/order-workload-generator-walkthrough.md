# Order Workload Generator — Implementation Walkthrough

This document records Checkpoint 1 in implementation order. The accepted Inventory implementation is a frozen dependency; no file under `src/Inventory.API`, `tests/Inventory.UnitTests`, its migrations, or its walkthrough is changed here.

## 1. Contract inspection before implementation

The real write endpoint is `POST /api/orders?api-version=1.0`. `Ordering.API/Program.cs` applies `RequireAuthorization()` to the route group, so a valid Bearer token is required. There is no additional named authorization/scope policy on this endpoint, although the token client requests the existing `orders` scope.

The JSON body fields are `userId`, `userName`, `city`, `street`, `state`, `country`, `zipCode`, `cardNumber`, `cardHolderName`, `cardExpiration`, `cardSecurityNumber`, `cardTypeId`, `buyer`, `items`, and `locationCode`. Every item carries `id`, `productId`, `productName`, `unitPrice`, `oldUnitPrice`, `quantity`, and `pictureUrl`. Success is an empty HTTP 200 response.

Every logical request must have a non-empty `x-requestid`. Ordering wraps that value in `IdentifiedCommand<CreateOrderCommand,bool>` and its request manager returns success for an already-recorded command, providing HTTP-command idempotency.

IdentityServer already supports OAuth and declares the `orders` API scope. Its checked-in clients use authorization-code or implicit grants; there was no client-credentials client. A conditional, secret-backed local client is added in the next section.

Catalog's usable one-request snapshot endpoint is `GET /api/catalog/items?api-version=1.0&pageSize=1000&pageIndex=0`. It returns `{ pageIndex, pageSize, count, data }`. Catalog is not authorization-protected and is queried before timing starts.

The existing Playwright configuration runs UI journeys serially with one browser worker and a saved authenticated state. There was no API load/performance generator.

## 2. Generator core

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/OrderLoadGenerator.csproj`
------------------------------------------------------------

**Why:** This creates a repository-native .NET executable while keeping a non-production load tool outside the application services.

**Meaningful code:** `OutputType=Exe` makes it runnable; `net10.0`, nullable analysis, and implicit usings match the repository. `Microsoft.AspNetCore.App` supplies hosting, configuration, options, and HTTP client infrastructure without another package. The ServiceDefaults reference supplies the repository's logging/OpenTelemetry baseline.

**Failure mode:** It is not part of the normal AppHost runtime unless explicitly enabled later.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/OrderGeneratorOptions.cs`
------------------------------------------------------------

**Responsibility:** Holds validated configuration for rate, duration, concurrency, seed, customer pool, Catalog page, hot/cold split, request/retry/drain timing, three service URLs, and OAuth identity.

Defaults are 10 requests/sec, 60 seconds, concurrency 50, and seed 42. Range attributes reject nonsensical values before load begins. `ClientSecret` has no default and therefore cannot accidentally be committed.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/appsettings.json`
------------------------------------------------------------

**Responsibility:** Supplies safe non-secret workload defaults and quiets per-request HTTP logs. It contains client ID/scope but no secret or token. URLs are also omitted because AppHost or the operator must supply real endpoints.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Generation/OrderScenario.cs`
------------------------------------------------------------

The records model the cached Catalog product, generated basket item, logical order scenario, and exact Ordering request. `RequestId` belongs to the logical order—not an individual HTTP attempt—so a retry reuses it. `TotalUnits` is derived for metrics.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Generation/OrderScenarioFactory.cs`
------------------------------------------------------------

**Data flow:** cached products + fixed seed → weighted location + hot/cold products + quantities + bounded customer → immutable scenario.

Products are sorted by ID so input enumeration order cannot destroy reproducibility. The first 20% form the hot set and receive 80% of selection attempts. A `HashSet<int>` guarantees 1–4 unique SKUs. Quantity is 1 for 80%, 2 for 15%, and 3 for 5%. Location thresholds implement NCR 40%, BLR 25%, BOM 20%, HYD 15%. Customer identity rotates through 200 deterministic buyers; this avoids 600 simultaneous attempts to create one new buyer while still exercising returning buyers.

The pseudo-random business sequence is repeatable. GUIDs remain fresh and unpredictable because idempotency identity must never repeat across separate runs.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Authentication/AccessTokenProvider.cs`
------------------------------------------------------------

**Responsibility:** Performs OAuth client credentials against `/connect/token`, caches the token, and refreshes before expiry.

The fast path returns a still-fresh cached token. `SemaphoreSlim` serializes refresh, and the second check after taking the lock prevents waiting workers from requesting another token. Form content carries client ID, secret, grant, and scope only to Identity. The token/secret are never logged. Refresh is scheduled before expiration.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Catalog/CatalogSnapshotProvider.cs`
------------------------------------------------------------

**Responsibility:** Makes exactly one Catalog request before the clock starts, filters invalid products, sorts them, and returns an in-memory snapshot. It fails fast for an empty response, no usable products, or a Catalog larger than the configured single-page snapshot.

Caching prevents Catalog latency from contaminating Ordering load or request latency measurements.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Ordering/OrderingClient.cs`
------------------------------------------------------------

**Responsibility:** Owns all HTTP mechanics: token, exact JSON, Bearer header, `x-requestid`, timeout, retry, latency, and result classification.

Each retry creates a new `HttpRequestMessage` but receives the same immutable scenario, so it reuses the same request GUID. Only 408, 429, and 5xx are retried. 4xx is an HTTP rejection; exhausted timeouts and transport failures have distinct classifications. A 2xx response is called HTTP accepted, never “paid.” The fake card data is fixed and future-dated; Ordering masks it before persistence.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Telemetry/LoadRunStatistics.cs`
------------------------------------------------------------

Thread-safe counters distinguish offered, accepted, HTTP failures, timeouts, dropped arrivals, duplicate request IDs, current/max in-flight, response statuses, locations, and order shape. Completed logical-request latency is stored for average and nearest-rank p50/p95/p99. `FormatSummary` prints the required explicit final report.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Generation/OrderRateWorker.cs`
------------------------------------------------------------

**Why open-loop matters:** A closed loop that waits for response then delays 100 ms would generate less load whenever Ordering slows. This implementation computes every target offset from `Stopwatch`, a monotonic clock unaffected by wall-clock changes. At 10/sec, arrivals are due at 0 ms, 100 ms, 200 ms, and so on through 59.9 seconds.

The producer never awaits request completion. It uses a bounded channel sized to maximum concurrency and `TryWrite`; once both workers and the bounded buffer cannot keep up, arrivals are counted as dropped rather than creating an unlimited queue. A fixed number of consumers bounds actual in-flight HTTP calls. After the offer window, the writer completes and consumers receive a configured drain window. Progress prints every five seconds.

`OrderRateWorker` performs Catalog and token preflight before starting the clock, runs once, prints the summary, sets a nonzero exit code for transport/load-generator failures, and requests graceful host shutdown.

------------------------------------------------------------
FILE: `tools/OrderLoadGenerator/Program.cs`
------------------------------------------------------------

The host adds repository telemetry, validated options, three named clients, singleton token/Catalog/Ordering/statistics/runner services, and the hosted worker. Explicit mappings support variables such as `ORDERGEN_RATE_PER_SECOND`, `ORDERGEN_DURATION_SECONDS`, and `ORDERGEN_CLIENT_SECRET`, while standard `OrderGenerator__...` .NET keys also remain available.

## 3. Generator unit tests

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/OrderLoadGenerator.UnitTests.csproj`
------------------------------------------------------------

This net10 MSTest executable references the generator and logging abstractions. It does not start eShop or pretend mocks prove integration.

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/GlobalUsings.cs`
------------------------------------------------------------

It imports MSTest and explicitly enables method-level parallelism, satisfying the repository analyzer while tests keep their own isolated state.

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/OrderScenarioFactoryTests.cs`
------------------------------------------------------------

Tests compare 1,000 scenarios from two seed-42 factories, validate supported locations, 1–4 unique lines, quantities 1–3, weighted location bands, strong hot-set skew, and 5,000 non-empty unique request IDs.

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/OrderingClientTests.cs`
------------------------------------------------------------

A recording HTTP handler returns 503 then 200. The test proves two attempts share one `x-requestid`, both carry the cached Bearer token, and both JSON bodies retain BLR.

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/LoadRunStatisticsTests.cs`
------------------------------------------------------------

Tests verify counts, offered rate, duplicate detection, averages, status counts, and nearest-rank p50=50, p95=95, p99=99 for values 1–100; an empty percentile returns zero.

------------------------------------------------------------
FILE: `tests/OrderLoadGenerator.UnitTests/OrderLoadRunnerTests.cs`
------------------------------------------------------------

The schedule test proves 600 arrivals and exact 100 ms spacing. A deliberately slow fake client at 200 offers/sec proves max in-flight never exceeds two, overload is reported as drops, all planned arrivals are counted, and shutdown drains to zero.

### Core checkpoint result

Initial compilation caught one unread constructor dependency; it was removed. Two first-run assertion mistakes in the tests—not product behavior—were corrected and rerun.

```text
dotnet build tools/OrderLoadGenerator/OrderLoadGenerator.csproj --no-restore -m:1
Build succeeded: 0 warnings, 0 errors

dotnet test tests/OrderLoadGenerator.UnitTests/OrderLoadGenerator.UnitTests.csproj --no-restore -p:BuildInParallel=false
8 passed, 0 failed, 0 skipped
```

## 4. Development-only authentication and opt-in AppHost wiring

------------------------------------------------------------
FILE: `src/Identity.API/Configuration/Config.cs`
------------------------------------------------------------

**Why:** Ordering requires a valid Bearer token. Reusing an interactive browser session would make a stable API workload fragile, so Identity conditionally exposes a machine client.

**Meaningful code:** Identity reads `OrderGenerator:ClientSecret`. Only when it is nonblank does it add client ID `order-generator`, `GrantTypes.ClientCredentials`, scope `orders`, the SHA-256 secret, and a ten-minute token lifetime. The secret itself is never stored in the repository. With no secret—the default—the client does not exist.

**Data flow:** Aspire secret parameter → Identity environment → hashed in-memory client definition; the same secret → generator environment → token form over the Identity endpoint.

**Failure modes:** Missing secret leaves the load resource unable to start and avoids accidentally enabling machine load credentials. A wrong secret produces a token preflight failure before the timed run.

------------------------------------------------------------
FILE: `src/eShop.AppHost/Extensions.cs`
------------------------------------------------------------

`IsOrderGeneratorEnabled` accepts only a parseable true value at `OrderGenerator:Enabled`. Missing, empty, false, or malformed values safely mean disabled.

------------------------------------------------------------
FILE: `src/eShop.AppHost/Program.cs`
------------------------------------------------------------

The existing OrderProcessor and PaymentProcessor builders were assigned to variables so load-only environment overrides can be attached without changing their appsettings defaults.

Inside the explicit enabled branch, AppHost creates a secret parameter, gives it to Identity and the generator, sets `BackgroundTaskOptions__GracePeriodTime=0`, `BackgroundTaskOptions__CheckUpdateTime=1`, and `PaymentOptions__PaymentSucceeded=true`, then starts the generator only after Identity, Catalog, and Ordering are ready. It supplies concrete Aspire endpoint references plus rate/duration/concurrency/seed.

Normal `dotnet run` does not enter this branch, so launching eShop cannot silently produce 10 orders/sec. The fast lifecycle values exist only in the opt-in resource environment; production/default JSON is untouched.

------------------------------------------------------------
FILE: `src/eShop.AppHost/eShop.AppHost.csproj`
------------------------------------------------------------

The tool project reference generates Aspire's typed `Projects.OrderLoadGenerator` symbol. A reference alone builds metadata; the conditional program branch controls whether it runs.

------------------------------------------------------------
FILE: `tests/eShop.AppHost.UnitTests/AppHostConfigurationTests.cs`
------------------------------------------------------------

Five data rows prove the opt-in parser: null, empty, invalid, and false remain disabled; only true enables it.

------------------------------------------------------------
FILE: `eShop.slnx`
------------------------------------------------------------

The generator is listed under `/tools/` and its tests under `/tests/`, making both discoverable by normal repository tooling.

------------------------------------------------------------
FILE: `e2e/CheckoutTest.spec.ts`
------------------------------------------------------------

The existing one-browser checkout journey now explicitly selects BLR and asserts the selected value before clicking Place order. Existing steps already prove authenticated login through saved setup state, item selection, basket, checkout, redirect, and one additional visible order. No workers or performance loops were added.

### Wiring checkpoint result

```text
Identity.API build: 0 warnings, 0 errors
AppHost full dependency build: 0 warnings, 0 errors
AppHost unit tests: 12 passed, 0 failed, 0 skipped
```

## 5. Runtime-discovered Ordering bottleneck

The first low-rate HTTP run itself succeeded: 10 offered, 10 accepted, exact 1.00/sec, zero failures/timeouts/drops/duplicate IDs, and process exit code 0. However, post-run lifecycle queries initially showed one AwaitingValidation, eight StockConfirmed, and only one Paid order.

Log inspection found the cause outside Inventory: `SetStockConfirmedOrderStatusCommandHandler`, `SetStockRejectedOrderStatusCommandHandler`, and `SetPaidOrderStatusCommandHandler` each deliberately waited a hard-coded 10 seconds to simulate work. RabbitMQ handled these messages serially, so even one order/sec built a long lifecycle backlog. Inventory was behaving correctly and was not modified.

------------------------------------------------------------
FILE: `src/Ordering.API/OrderingProcessingOptions.cs`
------------------------------------------------------------

**Why:** Preserve the demo's normal simulated delay while allowing the approved local workload profile to disable it.

`SectionName` is `OrderingProcessing`. `SimulatedDelayMilliseconds` defaults to 10,000, exactly preserving previous behavior when no configuration exists.

------------------------------------------------------------
FILE: `src/Ordering.API/Extensions/Extensions.cs`
------------------------------------------------------------

The new options section is bound, validated as non-negative, and validated at startup. Invalid runtime configuration fails visibly before message processing.

------------------------------------------------------------
FILES: `src/Ordering.API/Application/Commands/SetStockConfirmedOrderStatusCommandHandler.cs`, `SetStockRejectedOrderStatusCommandHandler.cs`, and `SetPaidOrderStatusCommandHandler.cs`
------------------------------------------------------------

Each handler now receives `IOptions<OrderingProcessingOptions>`. Its former `Task.Delay(10000)` becomes `DelayIfConfiguredAsync`: zero returns `Task.CompletedTask`; otherwise it awaits the configured milliseconds with cancellation. Repository lookup, state transition, transaction behavior, and event publication are unchanged.

------------------------------------------------------------
FILE: `src/Ordering.API/GlobalUsings.cs`
------------------------------------------------------------

Global imports for `Microsoft.Extensions.Options` and the API root namespace make the new options types available to handlers and the global-namespace extension class.

------------------------------------------------------------
FILE: `src/eShop.AppHost/Program.cs` (load branch)
------------------------------------------------------------

The opt-in branch now adds `OrderingProcessing__SimulatedDelayMilliseconds=0` to Ordering. This is alongside, and limited exactly like, the approved OrderProcessor and PaymentProcessor overrides. Default configuration remains 10 seconds.

### Corrected low-rate checkpoint

```text
Generator: 10 offered, 10 HTTP accepted, 1.00/sec
Failures/timeouts/drops/duplicate IDs: 0/0/0/0
Latency avg/p50/p95/p99: 124.2/23.8/970.5/970.5 ms
Ordering lifecycle after settle: Paid=10
Inventory movements: Reserve=28, Sale=28
Inventory invariant violations: 0
Ordering unit tests: 44 passed
AppHost unit tests: 12 passed
Affected builds: 0 warnings, 0 errors
```

## 6. One-browser Playwright preflight

Before applying load, we ran exactly one existing checkout journey through the real browser UI. Playwright used the repository's saved authenticated state, opened the catalog, added a product, opened the basket, selected BLR, submitted checkout, and verified that the visible order count increased.

```bash
USERNAME1='alice' PASSWORD='<local-development-password>' ESHOP_USE_HTTP_ENDPOINTS=1 \
  npx playwright test e2e/CheckoutTest.spec.ts --project='e2e tests logged in'
```

Result: 2 tests passed in 27.7 seconds. The two tests are the login setup and the single checkout scenario. Playwright was not used as the load engine, so browser rendering time cannot distort the 10-orders/sec measurement.

## 7. Full 10 orders/second workload

The validated workload was launched through the explicit AppHost opt-in. The secret below is intentionally a placeholder; the real local value was supplied only at runtime and was not written to a repository file.

```bash
env 'Parameters__order-generator-client-secret=<local-secret>' \
  ESHOP_USE_HTTP_ENDPOINTS=1 \
  OrderGenerator__Enabled=true \
  OrderGenerator__RatePerSecond=10 \
  OrderGenerator__DurationSeconds=60 \
  OrderGenerator__MaxConcurrency=50 \
  OrderGenerator__RandomSeed=42 \
  aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj --no-build
```

Runtime sequence:

1. AppHost started the normal eShop services and applied only the load-profile overrides.
2. The generator fetched one Catalog snapshot containing 101 usable SKUs.
3. It acquired one OAuth client-credentials token before starting the stopwatch.
4. The open-loop scheduler offered one request every 100 ms. Slow requests did not shift future target times.
5. Bounded workers submitted the exact Ordering API contract with unique `x-requestid` values.
6. Ordering published its normal lifecycle events through RabbitMQ; Inventory reserved and then sold stock.
7. After 60 seconds the generator stopped offering requests, drained in-flight work, printed its summary, and exited normally. AppHost remained available for lifecycle verification and was then stopped cleanly with `aspire stop`.

### Generator result

```text
Configured target:              10.00 attempts/sec for 60 seconds
Catalog snapshot:               101 SKUs
Offer window:                   60.00 seconds
Offered attempts:               600
Achieved offer rate:            10.00/sec
HTTP accepted:                  600 (10.00/sec)
HTTP failures:                  0
Timeouts:                       0
Locally dropped:                0
Duplicate request IDs:          0
Maximum in flight:              12 of 50
Latency average:                41.0 ms
Latency p50 / p95 / p99:        23.9 / 55.7 / 601.6 ms
Average lines / units:          2.53 / 3.18 per order
HTTP status counts:             200=600
Location counts:                BLR=142, BOM=129, HYD=84, NCR=245
```

`Offered` means the scheduler reached an arrival slot. `HTTP accepted` means Ordering returned success. A local drop would mean the bounded channel was full; none occurred. `Maximum in flight` staying at 12 proves the configured limit of 50 was respected. The tail latency is visible in p99 without reducing the offer rate because pacing is open-loop.

### Downstream business result

Read-only PostgreSQL checks after the message pipeline settled reported:

```text
Ordering Paid orders created by this run: 600
Inventory Reserve movement quantity:      1518
Inventory Sale movement quantity:         1518
Inventory invariant violations:           0
Rejected orders:                          0
Cancelled orders:                         0
```

Reserve and Sale quantities match, so every reserved unit from this successful run was committed exactly once. Rejected and cancelled counts are zero because the deterministic seed had enough available stock for this workload. Zero invariant violations confirms `0 <= reserved <= on_hand <= max_stock`, `0 <= safety_stock <= reorder_point <= max_stock`, and `available = on_hand - reserved` still held after concurrent processing.

No Inventory source file, migration, or test was changed during this checkpoint.

## 8. Final verification and file inventory

The final clean builds used one MSBuild node. An attempted parallel audit exposed a tooling-only project-graph failure with no compiler diagnostic; rerunning serially succeeded and avoids concurrent writes to the repository's shared `artifacts` directory.

```bash
dotnet build tools/OrderLoadGenerator/OrderLoadGenerator.csproj --no-restore
dotnet build src/Identity.API/Identity.API.csproj --no-restore
dotnet build src/Ordering.API/Ordering.API.csproj --no-restore --disable-build-servers -maxcpucount:1
dotnet build src/eShop.AppHost/eShop.AppHost.csproj --no-restore --disable-build-servers -maxcpucount:1

dotnet test tests/OrderLoadGenerator.UnitTests/OrderLoadGenerator.UnitTests.csproj --no-restore
dotnet test tests/Ordering.UnitTests/Ordering.UnitTests.csproj --no-restore
dotnet test tests/eShop.AppHost.UnitTests/eShop.AppHost.UnitTests.csproj --no-restore
```

Final results:

```text
Generator build:       0 warnings, 0 errors
Identity build:        0 warnings, 0 errors
Ordering build:        0 warnings, 0 errors
Full AppHost build:    0 warnings, 0 errors
Generator tests:       8 passed, 0 failed, 0 skipped
Ordering tests:        44 passed, 0 failed, 0 skipped
AppHost tests:         12 passed, 0 failed, 0 skipped
Playwright preflight:  2 passed
```

Created generator files:

- `tools/OrderLoadGenerator/OrderLoadGenerator.csproj`
- `tools/OrderLoadGenerator/Program.cs`
- `tools/OrderLoadGenerator/appsettings.json`
- `tools/OrderLoadGenerator/OrderGeneratorOptions.cs`
- `tools/OrderLoadGenerator/Authentication/AccessTokenProvider.cs`
- `tools/OrderLoadGenerator/Catalog/CatalogSnapshotProvider.cs`
- `tools/OrderLoadGenerator/Generation/OrderScenario.cs`
- `tools/OrderLoadGenerator/Generation/OrderScenarioFactory.cs`
- `tools/OrderLoadGenerator/Generation/OrderRateWorker.cs`
- `tools/OrderLoadGenerator/Ordering/OrderingClient.cs`
- `tools/OrderLoadGenerator/Telemetry/LoadRunStatistics.cs`

Created test and documentation files:

- `tests/OrderLoadGenerator.UnitTests/OrderLoadGenerator.UnitTests.csproj`
- `tests/OrderLoadGenerator.UnitTests/GlobalUsings.cs`
- `tests/OrderLoadGenerator.UnitTests/OrderScenarioFactoryTests.cs`
- `tests/OrderLoadGenerator.UnitTests/OrderingClientTests.cs`
- `tests/OrderLoadGenerator.UnitTests/LoadRunStatisticsTests.cs`
- `tests/OrderLoadGenerator.UnitTests/OrderLoadRunnerTests.cs`
- `docs/order-workload-generator-walkthrough.md`

Modified integration files:

- `src/Identity.API/Configuration/Config.cs`
- `src/eShop.AppHost/Extensions.cs`
- `src/eShop.AppHost/Program.cs`
- `src/eShop.AppHost/eShop.AppHost.csproj`
- `tests/eShop.AppHost.UnitTests/AppHostConfigurationTests.cs`
- `e2e/CheckoutTest.spec.ts`
- `eShop.slnx`

Modified Ordering files for the load-only delay override:

- `src/Ordering.API/OrderingProcessingOptions.cs` (created)
- `src/Ordering.API/Extensions/Extensions.cs`
- `src/Ordering.API/GlobalUsings.cs`
- `src/Ordering.API/Application/Commands/SetStockConfirmedOrderStatusCommandHandler.cs`
- `src/Ordering.API/Application/Commands/SetStockRejectedOrderStatusCommandHandler.cs`
- `src/Ordering.API/Application/Commands/SetPaidOrderStatusCommandHandler.cs`

## 9. Operational failure modes

- Generator disabled or secret missing: no load resource and no machine OAuth client are created.
- Wrong endpoint or credentials: preflight fails before the measured clock starts.
- Catalog returns no usable items: the run fails before offering traffic.
- Ordering slows down: in-flight work rises up to the bound; after saturation, arrivals are counted as local drops instead of consuming unlimited memory.
- Transient HTTP response: the client retries only the configured small number of times and reuses the same request ID, preserving Ordering idempotency.
- Timeout or transport error: metrics distinguish it from an application HTTP rejection.
- Insufficient inventory: Ordering can reject the order through its normal asynchronous lifecycle; HTTP acceptance alone is therefore reported separately from Paid outcomes.
- Invalid lifecycle delay: Ordering rejects negative configuration during startup.

Checkpoint 1 is complete. The next checkpoint is CDC source validation; no Debezium, Kafka, stream engine, or analytics implementation was started here.
