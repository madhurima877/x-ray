# X-Ray API (Go)

Message inspection service. The relay sends one message to `POST /inspect`. X-Ray answers `202 Accepted` in about 2 ms, works out a verdict in the background, and POSTs it back to the relay's callback URL. The service is stateless, so you can run many copies behind a load balancer (target: 4,000 → 20,000 msg/s).

## Layout

```
cmd/xray            main: wiring, HTTP server, graceful shutdown
cmd/mockrelay       local stand-in for the relay (logs callbacks)
internal/api        HTTP handler: steps 1–4, push to job channel
internal/pipeline   worker pool: steps 5–12
internal/store      Redis: API keys, customer copy, rate counters, lists, keywords, URL cache
internal/urls       URL extract + normalise (4), short-link expansion (8)
internal/keywords   Aho-Corasick matcher + background reloader (7)
internal/ai         client for the Python TinyBERT service (9)
internal/callback   verdict POST to relay with short retry (11)
internal/events     async Kafka producer (12)
internal/metrics    TPS, latency, queue → /metrics
ai-service/         stub of the Python scoring service (same contract)
scripts/seed.sh     demo customer, lists, keywords
```

## Flow

**HTTP handler goroutine** (one per request)

1. **Receive + validate**: `X-API-Key` header, `message_id` and `text` required. Bad request → `400`, unknown key → `401`.
2. **Rate limit**: Redis `INCR xray:rl:{customer}:{second}`. Over the limit → `200 {"verdict":"FAIL_OPEN"}`.
3. **Find level T1–T5**: reads the customer copy and policies from Redis (cached in memory for 10 s).
4. **Extract URLs**: regex, then normalise and deduplicate (max 10).
5. **Push job → channel** (buffered) → `202`. If the channel is full → `FAIL_OPEN`.

**Worker goroutines** (pool of `WORKERS`). Each job runs under `context.WithTimeout(4.5s)`.

| Step | What | Level |
|---|---|---|
| 5 | Verdict cache: one `MGET` for all URLs; a cached BLOCK stops here | all |
| 6 | Black List, then White List: sender + URL hosts in one pipeline. Black → BLOCK, stop. Whitelisted sender → ALLOW, stop | all |
| 7 | Keywords: clean text, Aho-Corasick, apply customer `keyword_action` | T2+ |
| 8 | One goroutine per URL (semaphore 4, 1 s each): follow redirects, re-check every hop against Black List | T4+ |
| 9 | AI score (300 ms); thresholds `ai_block` / `ai_hold` from customer policy | T3+ |
| 10 | Combine: BLOCK > HOLD > ALLOW, keep all reasons; hard deadline → FAIL_OPEN | |
| 11 | Callback to relay: POST verdict, retry 5xx/429 twice | |
| 12 | Publish result event to Kafka (async, batched, non-blocking) | |

Steps 8 and 9 run together under `errgroup.WithContext`. A BLOCK from any of them cancels the rest.

Levels add up: T1 = cache + lists, T2 adds keywords, T3 adds AI, T4 adds URL expansion, T5 adds strict mode (a URL that cannot be resolved → HOLD).

**Background goroutines:** keyword reloader (ticker + `atomic.Pointer` swap), Kafka producer, metrics (1 s ticker), and graceful shutdown. On SIGTERM, `/readyz` returns 503, the server stops accepting requests, the job channel is drained so every accepted message still gets its callback, and Kafka is flushed.

**Time budget:** steps 1–4 ≈ 2 ms · steps 5–7 ≈ 1–3 ms · steps 8 + 9 up to ≈ 1 s · hard stop 4.5 s (RFP max 5 s).

## API

```
POST /inspect
X-API-Key: demo-key
{"message_id":"m-1","sender":"+911234567890","text":"Claim now bit.ly/abc"}

202 {"message_id":"m-1","status":"ACCEPTED"}
200 {"message_id":"m-1","verdict":"FAIL_OPEN","reason":"queue full"}
```

Callback body sent to the customer's `callback_url`:

```json
{"message_id":"m-1","customer_id":"cust-demo","verdict":"HOLD","level":5,
 "reasons":[{"step":"keywords","action":"HOLD","detail":"free money"}],
 "ai_score":0.15,"model_version":"stub-tinybert-0.1","latency_ms":312.4,"decided_at":"..."}
```

Other endpoints: `GET /healthz`, `GET /readyz` (Redis ping, 503 while draining), `GET /metrics` (JSON).

## Redis data

| Key | Type | Content |
|---|---|---|
| `xray:apikey:{sha256(key)}` | string | customer ID |
| `xray:customer:{id}` | hash | `level` 1–5, `rate_limit`/s (0 = none), `callback_url`, `keyword_action` HOLD/BLOCK, `ai_block`, `ai_hold` |
| `xray:bl`, `xray:wl` | set | global Black/White List (hosts without `www.`, sender IDs) |
| `xray:bl:{id}`, `xray:wl:{id}` | set | per-customer lists |
| `xray:keywords` | set | keyword phrases |
| `xray:urlv:{hash}` | string | cached URL verdict (TTL `URL_CACHE_TTL`) |

## Run

```bash
docker compose up --build
```

```bash
curl -s localhost:8080/inspect -H 'X-API-Key: demo-key' -d '{"message_id":"m-1","sender":"+911","text":"You are a lottery winner, visit https://evil.example/x"}'
```

```bash
docker compose logs -f mockrelay
```

Locally, without Docker (Redis on `localhost:6379`, seeded with `scripts/seed.sh`):

```bash
go run ./cmd/xray
```

```bash
go test -race ./...
```

## Configuration (env)

| Var | Default | |
|---|---|---|
| `XRAY_ADDR` | `:8080` | |
| `REDIS_ADDR` / `REDIS_PASSWORD` / `REDIS_DB` / `REDIS_POOL_SIZE` | `localhost:6379` / – / 0 / 200 | |
| `KAFKA_BROKERS` / `KAFKA_TOPIC` | – / `xray.verdicts` | if no brokers are set, events are logged instead |
| `AI_URL` / `AI_TIMEOUT` | – / `300ms` | if no URL is set, the AI step is skipped |
| `WORKERS` / `QUEUE_SIZE` | 512 / 10000 | |
| `JOB_TIMEOUT` | `4.5s` | |
| `URL_TIMEOUT` / `URL_CONCURRENCY` / `URL_MAX_HOPS` | `1s` / 4 / 5 | |
| `ALLOW_PRIVATE_URLS` | false | URL expansion refuses private IPs (SSRF guard) |
| `CALLBACK_TIMEOUT` / `CALLBACK_RETRIES` | `2s` / 2 | |
| `KEYWORD_RELOAD` / `URL_CACHE_TTL` / `CUSTOMER_CACHE_TTL` | `30s` / `10m` / `10s` | |
| `SHUTDOWN_TIMEOUT` | `30s` | |
# x-ray
