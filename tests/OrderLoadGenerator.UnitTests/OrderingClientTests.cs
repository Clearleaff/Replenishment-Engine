using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Options;
using eShop.OrderLoadGenerator;
using eShop.OrderLoadGenerator.Authentication;
using eShop.OrderLoadGenerator.Generation;
using eShop.OrderLoadGenerator.Ordering;

namespace eShop.OrderLoadGenerator.UnitTests;

[TestClass]
public sealed class OrderingClientTests
{
    [TestMethod]
    public async Task Transient_retry_reuses_request_id_and_sends_real_contract_shape()
    {
        var requestId = Guid.NewGuid();
        var handler = new RecordingHandler(
            new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            new HttpResponseMessage(HttpStatusCode.OK));
        var client = new OrderingClient(
            new SingleClientFactory(new HttpClient(handler) { BaseAddress = new Uri("http://ordering/") }),
            new FixedTokenProvider(),
            Options.Create(new OrderGeneratorOptions { TransientRetries = 1, RequestTimeoutSeconds = 2 }));
        var scenario = new OrderScenario(
            requestId,
            "customer-1",
            "Customer One",
            "BLR",
            [new("1", 1, "Product 1", 10, 10, 2, null)]);

        var result = await client.SubmitAsync(scenario, CancellationToken.None);

        Assert.AreEqual(OrderSubmissionClassification.Accepted, result.Classification);
        Assert.AreEqual(2, result.Attempts);
        Assert.HasCount(2, handler.RequestIds);
        Assert.IsTrue(handler.RequestIds.All(id => id == requestId));
        Assert.IsTrue(handler.Authorization.All(header => header?.Scheme == "Bearer" && header.Parameter == "test-token"));
        Assert.IsTrue(handler.LocationCodes.All(code => code == "BLR"));
    }

    private sealed class FixedTokenProvider : IAccessTokenProvider
    {
        public Task<string> GetAccessTokenAsync(CancellationToken cancellationToken) => Task.FromResult("test-token");
    }

    private sealed class SingleClientFactory(HttpClient client) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => client;
    }

    private sealed class RecordingHandler(params HttpResponseMessage[] responses) : HttpMessageHandler
    {
        private int _responseIndex;
        public List<Guid> RequestIds { get; } = [];
        public List<AuthenticationHeaderValue?> Authorization { get; } = [];
        public List<string> LocationCodes { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestIds.Add(Guid.Parse(request.Headers.GetValues("x-requestid").Single()));
            Authorization.Add(request.Headers.Authorization);
            using var document = JsonDocument.Parse(await request.Content!.ReadAsStreamAsync(cancellationToken));
            LocationCodes.Add(document.RootElement.GetProperty("locationCode").GetString()!);
            return responses[_responseIndex++];
        }
    }
}
