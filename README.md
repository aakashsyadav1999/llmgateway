# llmgateway

A Go gateway for OpenAI-compatible LLM APIs. Give clients a gateway key, keep
provider credentials on the server, and route requests through shared rate
limits, response caching, retries, and per-upstream circuit breakers.

The executable is called `llmgate`. Redis enables shared rate limiting and
response caching; the gateway also runs without Redis using an in-memory limiter.

Open source under the [MIT License](LICENSE).

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick start with Docker Compose](#quick-start-with-docker-compose)
- [Run locally with Go](#run-locally-with-go)
- [Use a local model with Ollama](#use-a-local-model-with-ollama)
- [Live testing](#live-testing)
- [Configuration](#configuration)
- [Multiple upstreams and failover](#multiple-upstreams-and-failover)
- [Rate limiting and Redis outages](#rate-limiting-and-redis-outages)
- [Response caching](#response-caching)
- [Metrics and cost estimates](#metrics-and-cost-estimates)
- [Deployment notes and limitations](#deployment-notes-and-limitations)
- [Development and automated tests](#development-and-automated-tests)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features

- Server-sent event (SSE) streaming, with cancellation when the client disconnects.
- Client API-key authentication, request IDs, and structured JSON access logs.
- Per-client token-bucket request limits, backed by memory or atomic Redis Lua operations.
- Redis response caching for eligible chat and embedding requests.
- Retries with exponential backoff and jitter for selected transient failures.
- Round-robin upstream selection with independent circuit breakers and failover.
- Prometheus request, latency, cache, rate-limit, token, and estimated cost metrics.
- Graceful shutdown, a non-root Docker image, and Compose services for Redis and optional Ollama.

Requests under `/v1/` are forwarded to the configured provider. The gateway
preserves model names and request bodies; it does not translate between different
provider APIs. All upstreams in a pool must support the models and API format
that your clients use.

## Requirements

Choose one setup:

| Setup | Requirements |
| --- | --- |
| Docker Compose | Git, Docker with a running daemon, and Docker Compose v2 or later |
| Local development | Git, Go 1.27.1 or later, and optionally `redis-server` / `redis-cli` |

Use an OpenAI-compatible provider with a valid API key, or the optional Ollama
service with a downloaded model. The request examples use `curl` and a POSIX shell
such as Bash. Docker builds do not require Go installed on the host.

## Quick start with Docker Compose

### 1. Clone and configure

```bash
git clone https://github.com/aakashsyadav1999/llmgateway.git
cd llmgateway
cp .env.example .env
```

If you already have `.env`, edit it instead of overwriting it. For a single
provider, set:

```dotenv
LLMGATE_UPSTREAM_URL=https://api.openai.com
LLMGATE_UPSTREAM_API_KEY=your-provider-api-key
LLMGATE_CLIENT_API_KEYS=your-gateway-client-key
LLMGATE_PORT=8080
LLMGATE_CACHE_TTL=24h
```

Replace the placeholder keys. The provider key is used by the gateway when
calling upstream; clients send the gateway key. Generate a separate, strong
key for each client whose limits and usage you want to track separately.
Multiple client keys are comma-separated.

For another compatible provider, change the base URL. The gateway appends
`/v1/...`, so use `https://provider.example`, rather than
`https://provider.example/v1`, when the provider's endpoint is
`https://provider.example/v1/chat/completions`.

### 2. Start the stack

```bash
docker compose up --build -d
docker compose ps
curl -i http://localhost:8080/healthz
```

Compose starts gateway + Redis and waits for Redis health before starting the
gateway. Expect `HTTP 200` from `/healthz`. This checks that the gateway is serving
requests; it does not check provider credentials or readiness.

### 3. Send your first request

Replace `your-model` with a model your provider supports and use the client key
you configured:

```bash
curl -i http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer your-gateway-client-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "your-model",
    "messages": [{"role": "user", "content": "Say hello in one sentence."}],
    "temperature": 0
  }'
```

Expect a JSON response with `HTTP 200`. Requests are subject to your provider's
usage charges. Models that do not accept `temperature: 0` can still be proxied;
omit that parameter and those chat requests will bypass the cache.

### 4. View logs and stop

```bash
docker compose logs -f gateway
```

Press Ctrl-C to leave the log viewer. Stop the stack with:

```bash
docker compose down
```

Redis data survives `down` in a named volume. `docker compose down -v` also deletes
the stack's Redis and Ollama named volumes, including cached responses and models.
To apply `.env` changes, run `docker compose up -d` again; `docker compose restart`
does not apply new environment values.

## Run locally with Go

From the repository root:

```bash
go mod download
export LLMGATE_UPSTREAM_URL='https://provider.example'
export LLMGATE_UPSTREAM_API_KEY='your-provider-api-key'
export LLMGATE_CLIENT_API_KEYS='your-gateway-client-key'
go run ./cmd/llmgate
```

The gateway listens on `:8080`. Run the health check and request examples from
another terminal. Ctrl-C initiates graceful shutdown.

The Go program reads process environment variables; it does **not** load `.env`
automatically. Compose reads `.env` for its configuration. Use the exports above
for local Go runs.

To enable Redis locally, start it in a separate terminal:

```bash
redis-server --bind 127.0.0.1 --port 6379
```

Then set this before starting the gateway:

```bash
export LLMGATE_REDIS_ADDR='127.0.0.1:6379'
go run ./cmd/llmgate
```

If Redis is already running, use the existing instance or choose another port.
To build a standalone binary:

```bash
go build -o llmgate ./cmd/llmgate
./llmgate
```

The same environment variables apply to the binary. While it is running,
`./llmgate healthcheck` probes its `/healthz` endpoint using `LLMGATE_ADDR`.

## Use a local model with Ollama

In `.env`, set:

```dotenv
LLMGATE_UPSTREAM_URL=http://ollama:11434
LLMGATE_UPSTREAM_API_KEY=ollama
LLMGATE_CLIENT_API_KEYS=your-gateway-client-key
```

Remove any `LLMGATE_UPSTREAMS` setting so it does not override this URL. The
application requires a nonempty upstream key even for this local setup.

```bash
docker compose --profile ollama up --build -d
docker compose exec ollama ollama pull llama3.2
```

After the model pull completes, use `"model": "llama3.2"` in the request examples.
The service uses CPU by default, and models persist in the `ollama-data` volume.
Container startup does not imply that a model has been downloaded. Set
`OLLAMA_IMAGE` to pin a version; its default is `ollama/ollama:latest`.

## Live testing

These checks work against Compose or a local Go process. Use the same shell
throughout and substitute your values:

```bash
export GATEWAY_URL='http://localhost:8080'
export GATEWAY_KEY='your-gateway-client-key'
```

If you changed `LLMGATE_PORT`, use that port in `GATEWAY_URL`.

### Health and authentication

```bash
curl -i "$GATEWAY_URL/healthz"
curl -i "$GATEWAY_URL/v1/models"
```

Expect `200` for health and `401` for the API request without authentication.
Every response includes a generated `X-Request-Id` header for log correlation.

### JSON responses and cache hits

Create a request file with a model that supports `temperature: 0`:

```bash
cat > /tmp/llmgate-live-request.json <<'JSON'
{
  "model": "your-model",
  "messages": [{"role": "user", "content": "Say hello in one sentence."}],
  "temperature": 0
}
JSON
```

Run this command twice with Redis caching enabled:

```bash
curl -i "$GATEWAY_URL/v1/chat/completions" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/llmgate-live-request.json
```

Expect `200` on both requests, first `X-Cache: MISS`, then `X-Cache: HIT`.
An existing entry may make the first request a HIT too; change the message to
force a fresh key. Cache hits do not make another upstream call or add upstream
usage. To check expiry, set `LLMGATE_CACHE_TTL=1m` and recreate the gateway, make a
fresh request, then repeat it after its entry expires and expect MISS.

### Streaming

Replace the model below with one that supports streaming:

```bash
curl -N -i "$GATEWAY_URL/v1/chat/completions" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Cache-Control: no-store' \
  -d '{
    "model": "your-model",
    "messages": [{"role": "user", "content": "Count from one to ten."}],
    "stream": true,
    "stream_options": {"include_usage": true}
  }'
```

Expect SSE `data:` events as tokens arrive and a final `[DONE]` marker from
compatible providers. `-N` disables curl's output buffering; `no-store` bypasses
caching. If the provider does not support `stream_options.include_usage`, remove
that field. Streamed token accounting needs provider-supplied usage; the gateway
does not invent missing token counts.

### Rate limiting without provider calls

Temporarily set these values in `.env`, retaining your main client key alongside
a separate test key:

```dotenv
LLMGATE_CLIENT_API_KEYS=your-gateway-client-key,rate-probe
LLMGATE_RATE_LIMIT_BURST=3
LLMGATE_RATE_LIMIT_PER_SECOND=0.1
```

Apply them with `docker compose up -d`. For a local Go process, export the
variables and restart it. Then run:

```bash
for i in 1 2 3 4 5 6; do
  curl -s -o /dev/null -w '%{http_code}\n' \
    -H 'Authorization: Bearer rate-probe' \
    "$GATEWAY_URL/rate-limit-probe"
done
```

A fresh test key, with all requests run quickly, should return
`404, 404, 404, 429, 429, 429`. The first three pass authentication and rate
limiting but reach a deliberately nonexistent route; no provider is called.
The last three are limiter rejections. At this refill rate, a new token takes
about ten seconds. Reusing a key with an existing Redis bucket can change the
initial results; use a new test key or wait for refill. Restore normal limits
after testing.

### Metrics

```bash
curl -s "$GATEWAY_URL/metrics" |
  grep -E '^llmgate_(requests_total|cache_requests_total|rate_limit_rejections_total|tokens_total|estimated_cost_usd_total|usage_responses_total)'
```

Look for cache hits/misses, tokens, and limiter rejections. Cost series appear
after usage has been observed for a model with configured prices. Families with
no observations may have no samples yet. Compare token and cost counters before
and after a cache HIT; they should remain unchanged.

## Configuration

Settings use environment variables. Durations use Go syntax such as `250ms`,
`5s`, or `24h`. Numeric limits and timeouts must be positive; cache TTL additionally
accepts `0` or `0s` to disable caching.

| Variable | Default | Description |
| --- | --- | --- |
| `LLMGATE_ADDR` | `:8080` | Listen address for a local Go run |
| `LLMGATE_UPSTREAM_URL` | `https://api.openai.com` | Single provider base URL |
| `LLMGATE_UPSTREAM_API_KEY` | Required for a single upstream | Provider credential |
| `LLMGATE_UPSTREAMS` | Unset | JSON pool; overrides single-provider URL/key |
| `LLMGATE_CLIENT_API_KEYS` | Required | Comma-separated gateway client keys |
| `LLMGATE_REDIS_ADDR` | Unset | Redis `host:port`; unset uses memory and disables caching |
| `LLMGATE_RATE_LIMIT_BURST` | `20` | Per-client bucket capacity in requests |
| `LLMGATE_RATE_LIMIT_PER_SECOND` | `5` | Per-client token refill rate |
| `LLMGATE_REDIS_RETRY_AFTER` | `5s` | Pause before retrying Redis after a failure |
| `LLMGATE_CACHE_TTL` | `24h` | Cache lifetime; `0` disables caching |
| `LLMGATE_RETRY_MAX_ATTEMPTS` | `3` | Total upstream attempts including the first; `1` disables retries |
| `LLMGATE_RETRY_BASE_DELAY` | `250ms` | Initial exponential backoff ceiling with jitter |
| `LLMGATE_RETRY_MAX_DELAY` | `4s` | Maximum retry wait |
| `LLMGATE_BREAKER_THRESHOLD` | `5` | Consecutive upstream failures before opening a circuit |
| `LLMGATE_BREAKER_COOLDOWN` | `30s` | Delay before allowing a recovery probe |
| `LLMGATE_SHUTDOWN_TIMEOUT` | `10s` | Drain active requests before closing connections |
| `LLMGATE_RESPONSE_HEADER_TIMEOUT` | `120s` | Maximum wait for upstream headers |
| `LLMGATE_MAX_IDLE_CONNS_PER_HOST` | `100` | Idle upstream connections per host |
| `LLMGATE_MODEL_PRICES` | Unset | JSON rates by upstream name and requested model |

Compose-only settings:

| Variable | Default | Description |
| --- | --- | --- |
| `LLMGATE_PORT` | `8080` | Published host port, bound to `127.0.0.1` |
| `OLLAMA_IMAGE` | `ollama/ollama:latest` | Image for the optional Ollama service |

Compose fixes the internal gateway address to `:8080` and Redis address to
`redis:6379`, regardless of their `.env` values. Shell-exported variables take
precedence over `.env` during Compose interpolation. Single-quote JSON values
in `.env` to preserve them as literal strings.

## Multiple upstreams and failover

For Compose, add a single-line JSON value to `.env`:

```dotenv
LLMGATE_UPSTREAMS='[{"name":"primary","url":"https://provider-a.example","api_key":"key-a"},{"name":"secondary","url":"https://provider-b.example","api_key":"key-b"}]'
```

For local Go runs, export the equivalent value:

```bash
export LLMGATE_UPSTREAMS='[
  {"name":"primary","url":"https://provider-a.example","api_key":"key-a"},
  {"name":"secondary","url":"https://provider-b.example","api_key":"key-b"}
]'
```

Each entry requires an HTTP(S) `url` and a nonempty `api_key`. Names must be
unique; omitted names become `upstream-1`, `upstream-2`, and so on. URLs cannot
contain embedded credentials, query strings, or fragments. You can use the same
URL with different API keys. A global `LLMGATE_UPSTREAM_API_KEY` is not required
when a pool is configured.

Round-robin rotation and circuit breakers run independently in each gateway
process. Selection skips open circuits and reserves one half-open recovery probe
per upstream. Retries rotate through available upstreams within the total
attempt budget. If all circuits are unavailable, requests fail quickly with
`503` and `Retry-After`.

Upstream HTTP `408`, `429`, `500`, `502`, `503`, and `504`, and eligible transport
errors, can trigger retries. Backoff uses jitter; a numeric `Retry-After` is
honored only when it fits within the maximum retry wait. Ordinary client errors,
transport timeouts, and client disconnects are not retried. A started response
or stream is never replayed against another upstream. HTTP `429` can be retried
but does not count as a breaker failure; transport errors and upstream `5xx` do.

To observe rotation in logs, send several authenticated requests with
`Cache-Control: no-store`; cache hits skip upstream selection.

## Rate limiting and Redis outages

Limits count requests per client key, not tokens. Instances pointing to the same
Redis share atomic refill-and-consume decisions. Without Redis, limits are local
to each process. A cached request still consumes one rate-limit token.

Redis bucket names use `ratelimit:<SHA-256-of-client-key>`, with the full digest
encoded as 64 hexadecimal characters. The limiter stores token counts and refill
timestamps, not raw client credentials. All instances derive the same key.

When upgrading from the earlier raw-key format, buckets start fresh. Old buckets
remain until their existing TTL expires; the upgrade does not remove credentials
from earlier Redis backups. Upgrade all instances together to avoid old and new
versions enforcing separate buckets for the same client.

If Redis becomes unavailable, the gateway falls back to a bounded local memory
limiter and bypasses caching. Redis operations use short timeouts and are retried
after a cooldown. This keeps traffic flowing, but global quotas become approximate:
with N instances, local fallbacks can admit up to N times the configured allowance.
This policy does not enforce a strict global quota while Redis is down.

Use separate Redis instances for unrelated deployments; limiter keys are not
namespaced by deployment.

## Response caching

Caching requires Redis and a positive TTL.

| Request | Cache eligibility |
| --- | --- |
| `POST /v1/chat/completions` | Explicit `temperature: 0`, with `n` omitted or equal to `1` |
| `POST /v1/embeddings` | Eligible |
| Other paths/methods, or chat with temperature omitted/nonzero | Bypass |

The key hashes the client, path, and normalized whole JSON request, including
model, messages, tools, stream settings, and other parameters. Object field order
and whitespace do not change the key. Entries are additionally scoped to the
upstream pool, including names, URLs, and credentials.

Only successful `200` responses are stored. Responses over 1 MiB, failed copies,
and streams without a final `data: [DONE]` marker are skipped. Complete SSE
responses can be cached; a HIT replays the SSE body in one write instead of
reproducing the original timing. Temperature zero does not guarantee that a
provider will always return an identical answer.

| Header | Behavior |
| --- | --- |
| Response `X-Cache: HIT` | Served from Redis |
| Response `X-Cache: MISS` | No cached entry available; request forwarded |
| Response `X-Cache: BYPASS` | Cache read skipped |
| Request `Cache-Control: no-cache` | Skip reading, but store an eligible fresh response |
| Request `Cache-Control: no-store` | Skip both reading and writing |

With caching disabled, `X-Cache` is absent and logs report `cache=off`. Redis
failures serve uncached responses. Entries expire after `LLMGATE_CACHE_TTL`;
rotating a client key or changing the pool creates a new scope. Existing entries
retain the TTL they were written with. Set TTL to `0` to disable caching, or
delete specific `pool:*:cache:*` entries as an operator.

## Metrics and cost estimates

`GET /metrics` exposes Prometheus metrics without client authentication. Scrape
every instance and aggregate results in Prometheus. Registries and counters are
per process; counters reset on restart. Scrape requests themselves are excluded
from gateway request counts.

| Metric | Description |
| --- | --- |
| `llmgate_requests_total` | Completed requests by method, route, and status |
| `llmgate_request_duration_seconds` | Latency histogram, including full stream delivery |
| `llmgate_cache_requests_total` | Cache decisions by `hit`, `miss`, `bypass`, or `off` |
| `llmgate_rate_limit_rejections_total` | Gateway limiter rejections by client ID |
| `llmgate_tokens_total` | Observed `input`, `output`, and `cached_input` tokens |
| `llmgate_estimated_cost_usd_total` | Estimated upstream cost using configured prices |
| `llmgate_usage_responses_total` | Successful final responses with `priced`, `unpriced`, or `missing` usage |

Go and process metrics are also exposed. Route and method labels are bounded.
Client IDs are the first 16 bytes of SHA-256 of a client key, encoded as 32
hexadecimal characters. Raw API keys are not metric labels. Unconfigured model
names are grouped under `model="unpriced"` to bound cardinality.

### Configure prices

Prices are **USD per million tokens**, keyed by upstream name and requested
model. These numbers are illustrative; replace them with your provider's actual
rates. For the single-provider quick start, add this to `.env`:

```dotenv
LLMGATE_MODEL_PRICES='{"default":{"your-model":{"input_usd_per_million":2,"output_usd_per_million":5,"cached_input_usd_per_million":0.5}}}'
```

Use `default` for a single upstream, the configured name for pool entries, or `*`
for shared model prices. An exact upstream/model price overrides a `*` price.
Supply both input and output rates; omitted fields default to zero. Cached input
defaults to the input rate when no discount is configured. Zero rates are valid
for a free local model. Negative/nonfinite prices, unknown fields, and unknown
upstream names are rejected. Matching uses the requested model, or the response
model when the request did not supply one.

Cost is calculated as:

```text
((input - cached_input) × input_rate
 + cached_input × cached_input_rate
 + output × output_rate) / 1,000,000
```

`cached_input` is a subset of input; do not add it to input when counting total
tokens. JSON and incremental SSE usage events are observed with bounded memory.
Cache hits do not add tokens or cost, even though the replayed body may contain
original provider usage. Missing or invalid usage is counted as missing, rather
than estimated from response text.

These counters cover observed usage in successful final responses. Failed
attempts, retries, disconnects, and provider-side billing can differ. They are
operational estimates, not a persistent billing ledger.

### Example PromQL

Cache hit ratio, excluding bypass/off:

```promql
sum(rate(llmgate_cache_requests_total{result="hit"}[5m]))
/ sum(rate(llmgate_cache_requests_total{result=~"hit|miss"}[5m]))
```

95th-percentile request latency:

```promql
histogram_quantile(0.95,
  sum by (le) (rate(llmgate_request_duration_seconds_bucket[5m])))
```

Estimated cost per client over an hour:

```promql
sum by (client) (increase(llmgate_estimated_cost_usd_total[1h]))
```

## Deployment notes and limitations

- The gateway serves HTTP. Terminate HTTPS at an ingress or reverse proxy for remote clients.
- `/healthz` and `/metrics` bypass API-key authentication. Restrict metrics access at the ingress or network boundary.
- Compose publishes only the gateway, bound to host loopback. Redis and Ollama are internal services.
- The Redis client currently supports an address only; Redis authentication and TLS are not configurable. Use a trusted private Redis network.
- Redis holds response bodies and limiter state. Treat its data as sensitive; limiter bucket names contain client-key hashes rather than raw credentials.
- Request bodies are limited to 1 MiB. Streams have a response-header timeout but no total lifetime deadline.
- The container runs as UID/GID `65532:65532` in a scratch image with CA certificates and an executable healthcheck.
- Compose allows 30 seconds for shutdown. Increase `stop_grace_period` if you increase `LLMGATE_SHUTDOWN_TIMEOUT` beyond that.
- The repository does not currently provide a published container image; build it from source.

To build/run the gateway image without Compose:

```bash
docker build -t llmgate:local .
docker run --rm \
  -e LLMGATE_UPSTREAM_URL=https://provider.example \
  -e LLMGATE_UPSTREAM_API_KEY=your-provider-api-key \
  -e LLMGATE_CLIENT_API_KEYS=your-gateway-client-key \
  -p 127.0.0.1:8080:8080 \
  llmgate:local
```

This standalone example uses the in-memory limiter and does not enable caching.

## Development and automated tests

### Repository layout

```text
cmd/llmgate/             Server entry point and middleware wiring
internal/config/         Environment parsing and validation
internal/middleware/     Authentication, logging, request IDs, rate limiting
internal/proxy/          HTTP/SSE forwarding and upstream selection
internal/cache/          Redis response cache
internal/resilience/     Retry policy and circuit breaker
internal/observability/  Prometheus metrics and accounting
internal/usage/          Bounded JSON/SSE usage observation
tests/                   Gateway and Redis integration tests
```

### Run checks

```bash
go test ./...
go vet ./...
```

For Redis-backed integration tests, use a **dedicated test Redis**. For example,
start an ephemeral instance on a separate port in another terminal:

```bash
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

Then run:

```bash
LLMGATE_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -count=1 -v ./...
```

Redis-backed tests skip when their Redis address is unavailable; the default is
`localhost:6379`. Lua integration tests start their own Redis process on a Unix
socket and also need `redis-server` and `redis-cli` installed. Check verbose output
for skips before treating a run as full integration coverage. Race testing
requires a platform/toolchain that supports Go's race detector. Automated tests
use local mock providers, not your real provider credentials.

Validate Compose without starting services:

```bash
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.example --profile ollama config --quiet
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Gateway exits during startup | Read `docker compose logs gateway`; replace placeholder keys and check JSON/duration values |
| Port already in use | Stop the old gateway or change `LLMGATE_PORT` in Compose / `LLMGATE_ADDR` locally |
| API returns `401` | Send a configured gateway key, not the provider key; check logs for upstream authentication errors |
| Provider returns `404` | Check the base URL, appended `/v1/` path, and model name |
| `429` responses | Check limiter rejection metrics; an upstream can also return `429` |
| Cache reports BYPASS | Check Redis is enabled, the path is eligible, and chat temperature is explicitly zero |
| First cache test is already a HIT | Redis persists across restarts; change the message or wait for TTL expiry |
| Streams arrive all at once | Use `curl -N` and `Cache-Control: no-store`; check buffering in your ingress |
| Tokens or cost are absent | Check usage metadata, streaming usage support, and model prices |
| All upstreams return `503` | Inspect breaker logs, provider availability, and `Retry-After` |
| `.env` edits have no effect | Run `docker compose up -d`; for Go runs, export variables and restart |
| Redis tests skip | Start dedicated Redis, set `LLMGATE_TEST_REDIS_ADDR`, and install Redis CLI for Lua tests |

Before debugging unexpected behavior, check `git status --short` for stray files
and confirm that an older gateway process is not still holding the port.

## Contributing

Bug reports, documentation improvements, and focused pull requests are welcome
through the [GitHub repository](https://github.com/aakashsyadav1999/llmgateway).

For a bug report, include the setup method, relevant versions, steps to reproduce,
expected/actual behavior, and sanitized logs. Remove API keys, `.env` contents,
and private request/response data before sharing output.

For a contribution:

1. Fork the repository and create a branch for the change.
2. Keep changes focused and explain the resulting behavior.
3. Format Go changes with `gofmt`, run relevant tests and `go vet ./...`.
4. Use real local Redis for changes to shared limits or caching, and report skipped tests.
5. Update this README when configuration or public behavior changes.
6. Open a pull request with the problem, change, and validation results.

## License

Licensed under the [MIT License](LICENSE).

Copyright (c) 2026 Aakash Yadav.
