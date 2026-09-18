namespace eShop.Ordering.API.Application.Commands;

// Regular CommandHandler
public class SetStockRejectedOrderStatusCommandHandler(
    IOrderRepository orderRepository,
    IOptions<OrderingProcessingOptions> options) : IRequestHandler<SetStockRejectedOrderStatusCommand, bool>
{
    /// <summary>
    /// Handler which processes the command when
    /// Stock service rejects the request
    /// </summary>
    /// <param name="command"></param>
    /// <returns></returns>
    public async Task<bool> Handle(SetStockRejectedOrderStatusCommand command, CancellationToken cancellationToken)
    {
        // Simulate a work time for rejecting the stock
        await DelayIfConfiguredAsync(cancellationToken);

        var orderToUpdate = await orderRepository.GetAsync(command.OrderNumber);
        if (orderToUpdate == null)
        {
            return false;
        }

        orderToUpdate.SetCancelledStatusWhenStockIsRejected(command.OrderStockItems);

        return await orderRepository.UnitOfWork.SaveEntitiesAsync(cancellationToken);
    }

    private Task DelayIfConfiguredAsync(CancellationToken cancellationToken) =>
        options.Value.SimulatedDelayMilliseconds == 0
            ? Task.CompletedTask
            : Task.Delay(options.Value.SimulatedDelayMilliseconds, cancellationToken);
}


// Use for Idempotency in Command process
public class SetStockRejectedOrderStatusIdentifiedCommandHandler : IdentifiedCommandHandler<SetStockRejectedOrderStatusCommand, bool>
{
    public SetStockRejectedOrderStatusIdentifiedCommandHandler(
        IMediator mediator,
        IRequestManager requestManager,
        ILogger<IdentifiedCommandHandler<SetStockRejectedOrderStatusCommand, bool>> logger)
        : base(mediator, requestManager, logger)
    {
    }

    protected override bool CreateResultForDuplicateRequest()
    {
        return true; // Ignore duplicate requests for processing order.
    }
}
