namespace eShop.Ordering.API.Application.Commands;

// Regular CommandHandler
public class SetPaidOrderStatusCommandHandler(
    IOrderRepository orderRepository,
    IOptions<OrderingProcessingOptions> options) : IRequestHandler<SetPaidOrderStatusCommand, bool>
{
    /// <summary>
    /// Handler which processes the command when
    /// Shipment service confirms the payment
    /// </summary>
    /// <param name="command"></param>
    /// <returns></returns>
    public async Task<bool> Handle(SetPaidOrderStatusCommand command, CancellationToken cancellationToken)
    {
        // Simulate a work time for validating the payment
        await DelayIfConfiguredAsync(cancellationToken);

        var orderToUpdate = await orderRepository.GetAsync(command.OrderNumber);
        if (orderToUpdate == null)
        {
            return false;
        }

        orderToUpdate.SetPaidStatus();
        return await orderRepository.UnitOfWork.SaveEntitiesAsync(cancellationToken);
    }

    private Task DelayIfConfiguredAsync(CancellationToken cancellationToken) =>
        options.Value.SimulatedDelayMilliseconds == 0
            ? Task.CompletedTask
            : Task.Delay(options.Value.SimulatedDelayMilliseconds, cancellationToken);
}


// Use for Idempotency in Command process
public class SetPaidIdentifiedOrderStatusCommandHandler : IdentifiedCommandHandler<SetPaidOrderStatusCommand, bool>
{
    public SetPaidIdentifiedOrderStatusCommandHandler(
        IMediator mediator,
        IRequestManager requestManager,
        ILogger<IdentifiedCommandHandler<SetPaidOrderStatusCommand, bool>> logger)
        : base(mediator, requestManager, logger)
    {
    }

    protected override bool CreateResultForDuplicateRequest()
    {
        return true; // Ignore duplicate requests for processing order.
    }
}
