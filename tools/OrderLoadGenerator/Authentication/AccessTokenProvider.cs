using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace eShop.OrderLoadGenerator.Authentication;

public interface IAccessTokenProvider
{
    Task<string> GetAccessTokenAsync(CancellationToken cancellationToken);
}

public sealed class AccessTokenProvider(
    IHttpClientFactory httpClientFactory,
    IOptions<OrderGeneratorOptions> options,
    TimeProvider timeProvider) : IAccessTokenProvider
{
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private string? _accessToken;
    private DateTimeOffset _refreshAt;

    public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        if (HasFreshToken())
        {
            return _accessToken!;
        }

        await _refreshLock.WaitAsync(cancellationToken);
        try
        {
            if (HasFreshToken())
            {
                return _accessToken!;
            }

            var settings = options.Value;
            using var content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = settings.ClientId,
                ["client_secret"] = settings.ClientSecret,
                ["grant_type"] = "client_credentials",
                ["scope"] = settings.Scope
            });
            using var response = await httpClientFactory.CreateClient("identity")
                .PostAsync("connect/token", content, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException($"Identity token request failed with HTTP {(int)response.StatusCode}.");
            }

            var token = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken)
                ?? throw new InvalidOperationException("Identity returned an empty token response.");
            if (string.IsNullOrWhiteSpace(token.AccessToken) || token.ExpiresIn <= 0)
            {
                throw new InvalidOperationException("Identity returned an invalid access token response.");
            }

            _accessToken = token.AccessToken;
            var refreshLead = TimeSpan.FromSeconds(Math.Min(30, Math.Max(1, token.ExpiresIn / 10)));
            _refreshAt = timeProvider.GetUtcNow().AddSeconds(token.ExpiresIn) - refreshLead;
            return _accessToken;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private bool HasFreshToken() =>
        _accessToken is not null && timeProvider.GetUtcNow() < _refreshAt;

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);
}
