using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.Extensions.Options;
using eShop.OrderLoadGenerator.Authentication;
using eShop.OrderLoadGenerator.Generation;

namespace eShop.OrderLoadGenerator.Ordering;

public enum OrderSubmissionClassification
{
    Accepted,
    HttpRejected,
    Timeout,
    TransportFailure,
    UnexpectedResponse
}

public sealed record OrderSubmissionResult(
    Guid RequestId,
    OrderSubmissionClassification Classification,
    HttpStatusCode? StatusCode,
    TimeSpan Latency,
    int Attempts);

public interface IOrderingClient
{
    Task<OrderSubmissionResult> SubmitAsync(OrderScenario scenario, CancellationToken cancellationToken);
}

public sealed class OrderingClient(
    IHttpClientFactory httpClientFactory,
    IAccessTokenProvider tokenProvider,
    IOptions<OrderGeneratorOptions> options) : IOrderingClient
{
    public async Task<OrderSubmissionResult> SubmitAsync(OrderScenario scenario, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var maximumAttempts = options.Value.TransientRetries + 1;
        OrderSubmissionClassification lastClassification = OrderSubmissionClassification.TransportFailure;
        HttpStatusCode? lastStatusCode = null;

        for (var attempt = 1; attempt <= maximumAttempts; attempt++)
        {
            try
            {
                var token = await tokenProvider.GetAccessTokenAsync(cancellationToken);
                using var request = CreateRequest(scenario, token);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(options.Value.RequestTimeoutSeconds));
                using var response = await httpClientFactory.CreateClient("ordering")
                    .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);

                lastStatusCode = response.StatusCode;
                if (response.IsSuccessStatusCode)
                {
                    return new(scenario.RequestId, OrderSubmissionClassification.Accepted, response.StatusCode, stopwatch.Elapsed, attempt);
                }

                lastClassification = (int)response.StatusCode is >= 400 and < 500
                    ? OrderSubmissionClassification.HttpRejected
                    : OrderSubmissionClassification.UnexpectedResponse;
                if (!IsTransient(response.StatusCode) || attempt == maximumAttempts)
                {
                    return new(scenario.RequestId, lastClassification, response.StatusCode, stopwatch.Elapsed, attempt);
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                lastClassification = OrderSubmissionClassification.Timeout;
                if (attempt == maximumAttempts)
                {
                    return new(scenario.RequestId, lastClassification, null, stopwatch.Elapsed, attempt);
                }
            }
            catch (HttpRequestException) when (attempt < maximumAttempts)
            {
                lastClassification = OrderSubmissionClassification.TransportFailure;
            }
            catch (HttpRequestException)
            {
                return new(scenario.RequestId, OrderSubmissionClassification.TransportFailure, null, stopwatch.Elapsed, attempt);
            }
        }

        return new(scenario.RequestId, lastClassification, lastStatusCode, stopwatch.Elapsed, maximumAttempts);
    }

    private static HttpRequestMessage CreateRequest(OrderScenario scenario, string accessToken)
    {
        var payload = new CreateOrderPayload(
            scenario.CustomerId,
            scenario.CustomerName,
            "Bengaluru",
            "42 Load Test Avenue",
            "Karnataka",
            "India",
            "560001",
            "4111111111111111",
            scenario.CustomerName,
            new DateTime(2035, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            "111",
            1,
            scenario.CustomerId,
            scenario.Items,
            scenario.LocationCode);

        var request = new HttpRequestMessage(HttpMethod.Post, "api/orders?api-version=1.0")
        {
            Content = JsonContent.Create(payload)
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        request.Headers.Add("x-requestid", scenario.RequestId.ToString());
        return request;
    }

    private static bool IsTransient(HttpStatusCode statusCode) =>
        statusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests || (int)statusCode >= 500;
}
