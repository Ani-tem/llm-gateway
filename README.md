# llm-gateway

A single OpenAI-compatible endpoint in front of many LLM providers, written in Go. Callers authenticate with per-tenant API keys and are rate limited by a Redis token bucket.

Clients call one URL and pick a provider with a prefix on the model name, or use a route alias that falls over between providers:

```
mock/echo                      -> built-in fake provider (free, deterministic)
ollama/llama3.2                -> local Ollama
groq/llama-3.3-70b-versatile   -> Groq
openrouter/<org>/<model>       -> OpenRouter
gemini/<model>                 -> Gemini's OpenAI-compatible endpoint
auto                           -> route: groq, then ollama, then mock
chaos                          -> route: a mock that fails half the time, then mock
```

Every response carries `X-Gateway-Upstream` naming the target that served it.

## Run

```sh
docker compose up -d   # Redis, for rate limiting
make run
curl localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-dev-local' \
  -d '{"model":"mock/echo","messages":[{"role":"user","content":"hello"}]}'

# streaming
curl -N localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-dev-local' \
  -d '{"model":"mock/echo","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

`sk-dev-local` is the sample tenant in `config.json` and is for local use only. Create a real key with `go run ./cmd/gateway keygen` and add the printed hash as a new tenant with its own `rpm` and `burst`.

Providers are listed in `config.json`. A provider whose API key env var (for example `GROQ_API_KEY`) is not set is skipped at startup.

```sh
make test
```

## Design decisions

- **One adapter for every real provider.** Ollama, Groq, OpenRouter and Gemini all expose the OpenAI chat-completions format, so `provider.OpenAI` covers them with a different base URL and key.
- **Lossless passthrough.** The gateway types only the fields it needs (`model`, `stream`, `messages`) and forwards the rest as raw JSON, so tools, `response_format` and future fields work without a gateway change.
- **Pull-based streams.** `Stream.Recv()` instead of a channel: a slow client slows the upstream read (backpressure), and there is no producer goroutine to leak.
- **Cancellation end to end.** The client's request context is passed to the upstream call, so a client that disconnects mid-stream stops the upstream generation. There is a test for this.
- **Timeouts that respect streaming.** No `WriteTimeout` on the server and no `Client.Timeout` upstream, since both would cut long streams. Header timeouts bound the slow cases instead.
- **Error mapping.** Upstream 400/404/422/429 pass through as the client's problem. Everything else, including a 401 for the gateway's own key, becomes 502. If every target's breaker is open the client gets a 503.
- **Only key hashes are stored.** Tenants are looked up by SHA-256 of the presented key, so the config file never contains a usable key.
- **Token bucket in one Lua script.** Refill and spend happen atomically inside Redis, so concurrent requests across any number of gateway instances cannot overspend a bucket (50 parallel requests against a burst of 10 admit exactly 10). The script uses Redis's clock, so instance clock skew does not matter.
- **Retry, fail over or give up, by error type.** Network errors and 5xx are retried once with jittered exponential backoff, then the next target is tried. A 429 or a bad provider key skips straight to the next target. A 400-class error stops immediately, because the next provider would reject the same request.
- **Streams fail over only before the first byte.** Once part of an answer has reached the client, switching providers would corrupt it.
- **Circuit breaker per provider, in memory.** Five consecutive failures open it for 30 s, then a single probe decides whether it closes. A dead provider then costs microseconds instead of a retry cycle per request. State is per instance, so the hot path needs no network call.
- **A cancelled client request is not a provider failure.** It neither fails over nor counts toward the breaker.
- **Rate limiter fails open.** If Redis is unreachable, requests are allowed after a 250 ms wait rather than rejected; a limiter outage should not become a gateway outage.

## Roadmap

1. ~~OpenAI-compatible endpoint, provider adapters, streaming~~
2. ~~Per-tenant API keys and token-bucket rate limiting (Redis)~~
3. ~~Fallback routing, retries, per-provider circuit breaker~~
4. Response caching
5. Usage metering per key (Postgres)
6. Prometheus + Grafana metrics, k6 load test
