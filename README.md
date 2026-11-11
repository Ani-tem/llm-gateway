# llm-gateway

A single OpenAI-compatible endpoint in front of many LLM providers, written in Go. Callers authenticate with per-tenant API keys and are rate limited by a Redis token bucket. Providers are retried and failed over behind circuit breakers, repeated requests are answered from a Redis cache, and every request is metered per tenant in Postgres.

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

Every response carries `X-Gateway-Upstream` naming the target that served it, or `X-Gateway-Cache: hit` if it came from the cache.

## Run

```sh
docker compose up -d   # Redis, Postgres, Prometheus, Grafana
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

Each tenant can read its own usage, totalled per model:

```sh
curl 'localhost:8080/v1/usage?hours=24' -H 'Authorization: Bearer sk-dev-local'
```

Providers are listed in `config.json`. A provider whose API key env var (for example `GROQ_API_KEY`) is not set is skipped at startup.

```sh
make test              # no external services needed
make test-integration  # also tests against the Postgres from docker compose
```

## Observability

The gateway exposes Prometheus metrics on a separate port (`:9102/metrics`), so they are never reachable through the public API listener. `docker compose up -d` also starts Prometheus (http://localhost:9090) and Grafana (http://localhost:3000, no login, bound to localhost), which opens on a provisioned dashboard: request rate by status, latency percentiles, cache hit ratio, upstream attempts by provider and outcome, circuit breaker state, rate-limit rejections and token throughput.

Metric labels are limited to configured tenants, configured providers and status codes. The model name is client-controlled, so it is deliberately not a label; it would let a client create unbounded time series.

## Load test

`loadtest/chat.js` is a k6 script with three phases at a constant arrival rate: every prompt new, a small set of prompts repeated, and a route whose first provider fails 30% of the time.

```sh
go run ./cmd/gateway -config loadtest/config.json   # port 18080, mock upstream with 20 ms latency
RATE=2000 DURATION=15s make loadtest                # k6 runs in Docker
```

Results on an 8-core Apple Silicon laptop, 2,000 requests/s per phase, with Redis and Postgres in Docker and k6 in Docker:

| Phase | Median | p95 | p99 | Failed |
|---|---|---|---|---|
| Uncached (20 ms mock upstream) | 21.5 ms | 22.5 ms | 24.8 ms | 0% |
| Cached | 1.0 ms | 1.6 ms | 15.6 ms | 0% |
| Chaos (30% upstream failure) | 21.7 ms | 232 ms | 257 ms | 0% |

- The gateway adds about 1.5 ms to an uncached request at the median. That covers auth, the Redis rate-limit script, the cache lookup and write, and the Docker network hop from k6.
- Under 30% upstream failure no request failed. The tail is the retry backoff (100 to 200 ms) before a second attempt or failover.
- All 89,907 requests were metered to Postgres with none dropped, and the gateway used about 52 MB of memory.
- 2,000 requests/s is the highest rate tested, not a measured ceiling.
- k6 reported a few 3 s outliers and about 0.1% of scheduled requests not started. The gateway's own log has no request over 262 ms, so these come from k6's first connections through Docker's network to the host.

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
- **Exact-match response cache, scoped per tenant.** The key is a SHA-256 of the tenant and the request body, so one tenant can never be served, or detect through timing, another tenant's response. Entries expire after `cache_ttl_seconds`. Streamed requests and failures are not cached, and a client can opt out with `Cache-Control: no-cache`.
- **Concurrent misses share one upstream call.** `singleflight` collapses identical in-flight requests, which prevents a cache stampede. The shared call is detached from the first caller's cancellation so one client leaving cannot fail the others.
- **Redis memory is capped** with `allkeys-lru` eviction, so the cache cannot grow without bound.
- **Usage is an append-only event table, not counters.** One row per request (tenant, model, upstream, status, tokens, latency, cached, stream). Any breakdown is a SQL query later, and cache hits are recorded with their tokens so the saving is measurable.
- **Metering is off the request path.** Requests push an event onto an in-memory queue; one goroutine writes batches to Postgres with `COPY` every second, and flushes on shutdown. If the queue is full or Postgres is down, events are dropped and counted rather than slowing or failing requests. That trade is fine for dashboards; exact billing would need a durable log in front.
- **Stream token counts without changing the stream.** Providers report usage for a stream only when asked, so the gateway always adds `stream_options.include_usage`. If the client did not ask, the extra usage chunk is read and withheld.
- **`/v1/usage` takes the tenant from the API key**, never from a parameter, so tenants cannot read each other's usage.
- **Rate limiter fails open.** If Redis is unreachable, requests are allowed after a 250 ms wait rather than rejected; a limiter outage should not become a gateway outage. The cache degrades the same way, to "no cache".

## Roadmap

1. ~~OpenAI-compatible endpoint, provider adapters, streaming~~
2. ~~Per-tenant API keys and token-bucket rate limiting (Redis)~~
3. ~~Fallback routing, retries, per-provider circuit breaker~~
4. ~~Response caching~~
5. ~~Usage metering per key (Postgres)~~
6. ~~Prometheus + Grafana metrics, k6 load test~~
