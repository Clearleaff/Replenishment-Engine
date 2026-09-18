using System.Data;
using Npgsql;
using Microsoft.EntityFrameworkCore.Storage;

namespace eShop.Inventory.API.Application;

public sealed class InventoryService(InventoryContext context, ILogger<InventoryService> logger) : IInventoryService
{
    private const int MaxConcurrencyAttempts = 3;

    public async Task<InventoryBalanceResponse?> GetAsync(
        int skuId,
        string locationCode,
        CancellationToken cancellationToken)
    {
        if (skuId <= 0 || string.IsNullOrWhiteSpace(locationCode))
        {
            return null;
        }

        var normalized = Normalize(locationCode);
        return await context.InventoryBalances
            .AsNoTracking()
            .Where(balance => balance.SkuId == skuId && balance.LocationCode == normalized)
            .Select(balance => new InventoryBalanceResponse(
                balance.SkuId,
                balance.LocationCode,
                balance.OnHand,
                balance.Reserved,
                balance.OnHand - balance.Reserved,
                balance.SafetyStock,
                balance.ReorderPoint,
                balance.MaxStock,
                balance.Version,
                balance.UpdatedAt))
            .SingleOrDefaultAsync(cancellationToken);
    }

    public Task<InventoryOperationResult> ReserveAsync(
        ReserveInventoryRequest request,
        CancellationToken cancellationToken = default) =>
        ExecuteWithRetryAsync(() => ReserveOnceAsync(request, cancellationToken), cancellationToken);

    public Task<InventoryOperationResult> CommitAsync(
        CompleteInventoryRequest request,
        CancellationToken cancellationToken = default) =>
        ExecuteWithRetryAsync(
            () => CompleteOnceAsync(request, InventoryReservationStatus.Committed, InventoryMovementType.Sale, cancellationToken),
            cancellationToken);

    public Task<InventoryOperationResult> ReleaseAsync(
        CompleteInventoryRequest request,
        CancellationToken cancellationToken = default) =>
        ExecuteWithRetryAsync(
            () => CompleteOnceAsync(request, InventoryReservationStatus.Released, InventoryMovementType.Release, cancellationToken),
            cancellationToken);

    public Task<InventoryOperationResult> RestockAsync(
        RestockInventoryRequest request,
        CancellationToken cancellationToken = default) =>
        ExecuteWithRetryAsync(() => RestockOnceAsync(request, cancellationToken), cancellationToken);

    private async Task<InventoryOperationResult> ReserveOnceAsync(
        ReserveInventoryRequest request,
        CancellationToken cancellationToken)
    {
        var validation = Validate(request.OperationId, request.OrderId, request.LocationCode);
        if (validation is not null || request.Items is null || request.Items.Count == 0 || request.Items.Any(i => i.SkuId <= 0 || i.Quantity <= 0))
        {
            return validation ?? new(InventoryOperationOutcome.Invalid, "At least one positive SKU quantity is required.");
        }

        var location = Normalize(request.LocationCode);
        var items = request.Items
            .GroupBy(item => item.SkuId)
            .Select(group => new InventoryItemRequest(group.Key, group.Sum(item => item.Quantity)))
            .OrderBy(item => item.SkuId)
            .ToArray();

        await using var transaction = await BeginTransactionIfNeededAsync(cancellationToken);
        if (await IsDuplicateAsync(request.OperationId, InventoryMovementType.Reserve, cancellationToken))
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.AlreadyProcessed, "Reservation operation was already processed.");
        }

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

        var balances = await LockBalancesAsync(location, items.Select(item => item.SkuId).ToArray(), cancellationToken);
        var found = balances.ToDictionary(balance => balance.SkuId);
        var missing = items.Where(item => !found.ContainsKey(item.SkuId)).Select(item => item.SkuId).ToArray();
        if (missing.Length > 0)
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.NotFound, "One or more SKU/location balances were not found.", missing);
        }

        var unavailable = items.Where(item => item.Quantity > found[item.SkuId].Available).Select(item => item.SkuId).ToArray();
        if (unavailable.Length > 0)
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.InsufficientStock, "The order cannot be reserved in full.", unavailable);
        }

        var now = DateTime.UtcNow;
        foreach (var item in items)
        {
            var balance = found[item.SkuId];
            balance.Reserve(item.Quantity, now);
            context.InventoryReservations.Add(new InventoryReservation(request.OrderId, item.SkuId, location, item.Quantity, now));
            AddMovement(request.OperationId, balance, request.OrderId, InventoryMovementType.Reserve, item.Quantity, now, "Order reservation");
        }

        await context.SaveChangesAsync(cancellationToken);
        await CommitIfOwnedAsync(transaction, cancellationToken);
        return new(InventoryOperationOutcome.Succeeded, "Inventory reserved.");
    }

    private async Task<InventoryOperationResult> CompleteOnceAsync(
        CompleteInventoryRequest request,
        InventoryReservationStatus targetStatus,
        InventoryMovementType movementType,
        CancellationToken cancellationToken)
    {
        var validation = Validate(request.OperationId, request.OrderId, request.LocationCode);
        if (validation is not null)
        {
            return validation;
        }

        var location = Normalize(request.LocationCode);
        await using var transaction = await BeginTransactionIfNeededAsync(cancellationToken);
        if (await IsDuplicateAsync(request.OperationId, movementType, cancellationToken))
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.AlreadyProcessed, "Lifecycle operation was already processed.");
        }

        var reservations = await context.InventoryReservations
            .Where(reservation => reservation.OrderId == request.OrderId && reservation.LocationCode == location)
            .OrderBy(reservation => reservation.SkuId)
            .ToListAsync(cancellationToken);
        if (reservations.Count == 0)
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.NotFound, "No reservation exists for this order and location.");
        }

        if (reservations.All(reservation => reservation.Status == targetStatus))
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.AlreadyProcessed, "Reservation already has the requested terminal state.");
        }

        if (reservations.Any(reservation => reservation.Status != InventoryReservationStatus.Reserved))
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.Conflict, "Reservation is already in a different terminal state.");
        }

        var balances = await LockBalancesAsync(location, reservations.Select(r => r.SkuId).ToArray(), cancellationToken);
        var bySku = balances.ToDictionary(balance => balance.SkuId);
        var now = DateTime.UtcNow;
        foreach (var reservation in reservations)
        {
            var balance = bySku[reservation.SkuId];
            if (targetStatus == InventoryReservationStatus.Committed)
            {
                balance.CommitSale(reservation.Quantity, now);
                reservation.Commit(now);
            }
            else
            {
                balance.Release(reservation.Quantity, now);
                reservation.Release(now);
            }

            AddMovement(request.OperationId, balance, request.OrderId, movementType, reservation.Quantity, now, $"Order {targetStatus}");
        }

        await context.SaveChangesAsync(cancellationToken);
        await CommitIfOwnedAsync(transaction, cancellationToken);
        return new(InventoryOperationOutcome.Succeeded, targetStatus == InventoryReservationStatus.Committed ? "Sale committed." : "Reservation released.");
    }

    private async Task<InventoryOperationResult> RestockOnceAsync(
        RestockInventoryRequest request,
        CancellationToken cancellationToken)
    {
        if (request.OperationId == Guid.Empty || request.SkuId <= 0 || request.Quantity <= 0 || string.IsNullOrWhiteSpace(request.LocationCode))
        {
            return new(InventoryOperationOutcome.Invalid, "Operation ID, SKU, location, and positive quantity are required.");
        }

        var location = Normalize(request.LocationCode);
        await using var transaction = await BeginTransactionIfNeededAsync(cancellationToken);
        if (await IsDuplicateAsync(request.OperationId, InventoryMovementType.Restock, cancellationToken))
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.AlreadyProcessed, "Restock operation was already processed.");
        }

        var balance = (await LockBalancesAsync(location, [request.SkuId], cancellationToken)).SingleOrDefault();
        if (balance is null)
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.NotFound, "SKU/location balance was not found.");
        }

        var now = DateTime.UtcNow;
        var accepted = balance.Restock(request.Quantity, now);
        if (accepted == 0)
        {
            await RollbackIfOwnedAsync(transaction, cancellationToken);
            return new(InventoryOperationOutcome.Conflict, "Balance is already at maximum stock.");
        }

        AddMovement(request.OperationId, balance, null, InventoryMovementType.Restock, accepted, now, request.Reason);
        await context.SaveChangesAsync(cancellationToken);
        await CommitIfOwnedAsync(transaction, cancellationToken);
        return new(InventoryOperationOutcome.Succeeded, accepted == request.Quantity ? "Inventory restocked." : $"Inventory capped at maximum stock; accepted {accepted} units.");
    }

    private async Task<List<InventoryBalance>> LockBalancesAsync(string locationCode, int[] skuIds, CancellationToken cancellationToken) =>
        await context.InventoryBalances
            .FromSqlInterpolated($"SELECT * FROM inventory.inventory_balances WHERE location_code = {locationCode} AND sku_id = ANY ({skuIds}) ORDER BY sku_id FOR UPDATE")
            .ToListAsync(cancellationToken);

    private Task<IDbContextTransaction?> BeginTransactionIfNeededAsync(CancellationToken cancellationToken) =>
        context.Database.CurrentTransaction is null
            ? BeginOwnedTransactionAsync(cancellationToken)
            : Task.FromResult<IDbContextTransaction?>(null);

    private async Task<IDbContextTransaction?> BeginOwnedTransactionAsync(CancellationToken cancellationToken) =>
        await context.Database.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

    private static Task CommitIfOwnedAsync(IDbContextTransaction? transaction, CancellationToken cancellationToken) =>
        transaction?.CommitAsync(cancellationToken) ?? Task.CompletedTask;

    private static Task RollbackIfOwnedAsync(IDbContextTransaction? transaction, CancellationToken cancellationToken) =>
        transaction?.RollbackAsync(cancellationToken) ?? Task.CompletedTask;

    private Task<bool> IsDuplicateAsync(Guid operationId, InventoryMovementType movementType, CancellationToken cancellationToken) =>
        context.InventoryMovements.AnyAsync(movement => movement.SourceEventId == operationId && movement.MovementType == movementType, cancellationToken);

    private void AddMovement(Guid operationId, InventoryBalance balance, int? orderId, InventoryMovementType type, int quantity, DateTime occurredAt, string? reason) =>
        context.InventoryMovements.Add(new InventoryMovement(Guid.NewGuid(), operationId, balance.SkuId, balance.LocationCode, orderId, type, quantity, occurredAt, balance.Version, reason));

    private async Task<InventoryOperationResult> ExecuteWithRetryAsync(
        Func<Task<InventoryOperationResult>> operation,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= MaxConcurrencyAttempts; attempt++)
        {
            try
            {
                if (context.Database.CurrentTransaction is not null)
                {
                    return await operation();
                }

                var strategy = context.Database.CreateExecutionStrategy();
                return await strategy.ExecuteAsync(operation);
            }
            catch (PostgresException exception) when (
                exception.SqlState == PostgresErrorCodes.UniqueViolation &&
                context.Database.CurrentTransaction is null)
            {
                logger.LogInformation(exception, "Duplicate inventory operation was stopped by a database uniqueness constraint");
                context.ChangeTracker.Clear();
                return new(InventoryOperationOutcome.AlreadyProcessed, "Operation was already processed.");
            }
            catch (Exception exception) when (exception is DbUpdateConcurrencyException ||
                                              exception is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure })
            {
                if (attempt == MaxConcurrencyAttempts)
                {
                    logger.LogWarning(exception, "Inventory concurrency retry limit reached");
                    context.ChangeTracker.Clear();
                    return new(InventoryOperationOutcome.Conflict, "Inventory changed concurrently; retry the operation.");
                }

                logger.LogWarning(exception, "Inventory concurrency conflict; retrying attempt {Attempt}", attempt + 1);
                context.ChangeTracker.Clear();
                await Task.Delay(TimeSpan.FromMilliseconds(20 * attempt), cancellationToken);
            }
        }

        throw new InvalidOperationException("Concurrency retry loop ended unexpectedly.");
    }

    private static InventoryOperationResult? Validate(Guid operationId, int orderId, string locationCode)
    {
        if (operationId == Guid.Empty || orderId <= 0 || string.IsNullOrWhiteSpace(locationCode))
        {
            return new(InventoryOperationOutcome.Invalid, "Operation ID, positive order ID, and location are required.");
        }

        return null;
    }

    private static string Normalize(string locationCode) => locationCode.Trim().ToUpperInvariant();
}
