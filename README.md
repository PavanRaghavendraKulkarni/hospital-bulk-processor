# Hospital Bulk Processor

A FastAPI service that accepts a CSV of hospitals and bulk-creates them through the
[Hospital Directory API](https://hospital-directory.onrender.com/docs). Uploads are validated
synchronously, then processed in the background by an [arq](https://arq-docs.helpmanual.io/) worker.
Live progress is tracked in Redis and exposed via a status endpoint and a Server-Sent Events stream.

- **Robust upstream calls**: timeouts, exponential backoff with jitter on network errors / 429 / 5xx, never on 4xx.
- **Two-level concurrency control**: up to 5 parallel calls per job, plus a **global** cap shared by every job and worker (held in Redis).
- **Idempotent uploads**: re-uploading the same file returns the existing batch instead of creating duplicates.
- **Crash-safe**: a restarted job skips rows already created and reconciles rows created upstream but not yet recorded.
- **Safe activation**: a batch is only activated when every row succeeded (configurable).

---

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

| Service  | What it is                                   |
|----------|----------------------------------------------|
| `api`    | FastAPI on http://localhost:8000 (Swagger at `/docs`) |
| `worker` | arq worker (`arq app.worker.WorkerSettings`) |
| `redis`  | Redis 7 with AOF persistence (`appendonly yes`, `appendfsync everysec`) on a named volume |

```bash
curl localhost:8000/health                       # {"status":"ok","redis":"ok"}
curl -F "file=@sample.csv" localhost:8000/hospitals/bulk
```

### Local development and tests

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q            # 129 tests, ~1.5s, never touches the real API
.venv/bin/ruff check app tests
```

Tests use **fakeredis** (with Lua, for the rate limiter) and **respx** to fake the upstream API.
`tests/upstream.py` is a scripted fake of the upstream API that can inject 5xx/4xx responses, add latency,
and record peak concurrency.

---

## Architecture

```mermaid
flowchart LR
    Client([Client]) -->|POST /hospitals/bulk<br/>CSV| API
    Client -->|GET status / SSE events| API

    subgraph Service
        API[FastAPI API<br/>validate · hash · store · enqueue]
        Worker[arq Worker<br/>BulkJobProcessor]
    end

    subgraph Redis[Redis · AOF]
        Queue[(arq queue)]
        Store[(bulk:* job state)]
        Limiter[(upstream:inflight<br/>global leases)]
    end

    API -->|1. meta + rows| Store
    API -->|2. enqueue batch_id| Queue
    Queue --> Worker
    Worker <-->|rows · results · counters| Store
    Worker <-->|acquire / release slot| Limiter
    Worker -->|POST /hospitals/<br/>PATCH .../activate<br/>GET /hospitals/batch/id| Upstream[(Hospital Directory API<br/>Render free tier)]
    API -->|read progress| Store
```

### Upload and processing flow

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant R as Redis
    participant W as Worker
    participant U as Upstream API

    C->>A: POST /hospitals/bulk (CSV)
    A->>A: validate (type, size, UTF-8/BOM, header, rows, limit)
    alt invalid
        A-->>C: 400 / 422 with row-level errors (nothing stored)
    end
    A->>R: create meta + rows (MULTI), TTL 24h
    A->>R: SET bulk:hash:{sha256} NX
    alt same file already queued/processing/completed
        A->>R: delete own just-created keys
        A-->>C: 202 existing batch_id, duplicate=true
    end
    A->>R: enqueue job (job_id = batch_id)
    A-->>C: 202 batch_id + status_url

    W->>R: status=processing, load rows + results
    opt job ran before (crash / retry)
        W->>U: GET /hospitals/batch/{id}
        W->>R: record rows found upstream as created
    end
    par up to 5 per job, N globally
        W->>R: acquire global slot (Lua)
        W->>U: POST /hospitals/ (retry 5xx/429/network)
        W->>R: release slot
        W->>R: HSET result + HINCRBY counters (MULTI)
    end
    alt 0 failures (or ACTIVATE_ON_PARTIAL_FAILURE)
        W->>U: PATCH /hospitals/batch/{id}/activate
    end
    W->>R: final status, finished_at, processing_time
```

### Code layout

```
app/
  config.py               all settings, from env (pydantic-settings)
  main.py                 app factory, lifespan, exception handlers
  routes/                 thin HTTP handlers (health, bulk)
  services/bulk_service.py    upload / validate / retry / status / SSE use cases
  services/bulk_processor.py  the job: resume, reconcile, create rows, activate
  clients/hospital_api.py     async httpx client with retries
  csv_validation.py       synchronous CSV validation
  progress_store.py       ALL Redis access for job state
  rate_limiter.py         global in-flight limiter (Redis + Lua)
  job_queue.py            arq behind a small protocol
  worker.py               arq entrypoint, retry policy, wiring
  models.py / schemas.py  domain models / API response models
```

Route handlers contain no business logic. Services depend on small protocols (`JobQueue`, `UpstreamLimiter`),
which is what makes the fast, network-free tests possible.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/hospitals/bulk` | Upload CSV → `202` with `batch_id` and `status_url` |
| `GET` | `/hospitals/bulk/{batch_id}/status` | Progress, counters, per-row results, timestamps |
| `GET` | `/hospitals/bulk/{batch_id}/events` | Server-Sent Events: `progress` on every change, then `complete` |
| `POST` | `/hospitals/bulk/{batch_id}/retry` | Re-queue only failed rows, activate when all succeed |
| `POST` | `/hospitals/bulk/validate` | Validate a CSV without processing it |
| `GET` | `/health` | Liveness + Redis connectivity (503 if Redis is down) |

**CSV format**: header `name,address,phone` (case/order-insensitive, `phone` column optional), UTF-8 (a BOM is
fine), at most `MAX_CSV_ROWS` (20) data rows and `MAX_UPLOAD_BYTES` (1 MiB). Row numbers in errors and results are
1-based data rows (the header is not counted).

### Upload

```bash
curl -F "file=@sample.csv" localhost:8000/hospitals/bulk
```
```json
{"batch_id":"a2cd6643-77d7-469e-9e00-dba2d124a0f1","status":"queued","total_rows":5,
 "duplicate":false,"status_url":"/hospitals/bulk/a2cd6643-77d7-469e-9e00-dba2d124a0f1/status"}
```

Uploading the same file again while that job is queued, processing or completed returns the same `batch_id`
with `"duplicate": true`.

### Validation errors

```bash
curl -F "file=@sample_invalid.csv" localhost:8000/hospitals/bulk      # → 422
```
```json
{"detail":"CSV contains invalid rows","errors":[
  {"row":2,"field":"name","message":"name is required"},
  {"row":3,"field":"address","message":"address is required"},
  {"row":4,"field":"phone","message":"phone contains invalid characters"},
  {"row":5,"field":null,"message":"Duplicate of row 1 (same name and address)"}]}
```

File-level problems (not `.csv`, too large, not UTF-8, empty, bad header, header only, too many rows) return
**400**. Row-level problems return **422** with every bad row listed, not just the first one.

### Status

```bash
curl localhost:8000/hospitals/bulk/$BATCH_ID/status
```
```json
{"batch_id":"a2cd...","filename":"sample.csv","status":"completed","total_rows":5,
 "processed":5,"succeeded":5,"failed":0,"percent_complete":100.0,"batch_activated":true,
 "created_at":"2026-10-30T15:06:52.683Z","started_at":"2026-10-30T15:06:52.781Z",
 "finished_at":"2026-10-30T15:06:59.606Z","processing_time_seconds":6.82,"error":null,
 "results":[{"row":1,"name":"Riverside General Hospital","status":"created","hospital_id":378,"error":null}, ...]}
```

`status` is one of `queued`, `processing`, `completed`, `partial_failure` or `failed`. Unknown batch → **404**.

### Live progress (SSE)

```bash
curl -N localhost:8000/hospitals/bulk/$BATCH_ID/events
```
```
event: progress
data: {"status":"processing","processed":2,"total_rows":5,"percent_complete":40.0,...}

event: complete
data: {"status":"completed","processed":5,...,"batch_activated":true}
```

In a browser: `new EventSource("/hospitals/bulk/<id>/events")`.

### Retry failed rows

```bash
curl -X POST localhost:8000/hospitals/bulk/$BATCH_ID/retry
```
```json
{"batch_id":"...","status":"queued","retried_rows":[2],"status_url":"/hospitals/bulk/.../status"}
```

Returns **409** while the job is queued or processing, or when it already completed. Concurrent retry calls are
safe: an atomic compare-and-set on `status` lets only one of them through.

### Validate only

```bash
curl -F "file=@sample.csv" localhost:8000/hospitals/bulk/validate
# {"valid":true,"total_rows":5,"rows":[{"row_number":1,"name":"Riverside General Hospital",...}]}
```

---

## Redis key design

| Key | Type | Contents | TTL |
|---|---|---|---|
| `bulk:{batch_id}:meta` | hash | `JobMeta` fields (status, filename, file_hash, total_rows, timestamps, `batch_activated`, error) **and** the `processed` / `succeeded` / `failed` counters | `JOB_TTL_SECONDS` (24h) |
| `bulk:{batch_id}:rows` | hash | `row_number → HospitalRow JSON`: the parsed upload | 24h |
| `bulk:{batch_id}:results` | hash | `row_number → RowResult JSON` (`created` + `hospital_id`, or `failed` + `error`) | 24h |
| `bulk:hash:{sha256}` | string | `batch_id` of the canonical job for that file content | `IDEMPOTENCY_TTL_SECONDS` (24h) |
| `upstream:inflight` | sorted set | global limiter leases: member = token, score = lease expiry | refreshed on every acquire |
| `arq:*` | (arq) | queue, job and result keys managed by arq | arq defaults |

Why this layout:
- **A `bulk:{batch_id}:` prefix per job**: everything about a job is easy to find, expire or delete together, and `bulk:*` never collides with arq's keys.
- **Hashes keyed by row number**: the worker, the retry endpoint and resume logic all need random access to individual rows ("has row 7 been created?"). A list or a JSON blob would force read-modify-write.
- **Counters live in the meta hash** and are updated with `HINCRBY` in the same `MULTI` as the row result, so `processed == len(results)` holds even with many rows finishing at once.
- **The idempotency key is content-addressed** (`sha256`), so the API can dedupe without parsing anything or scanning jobs.

---

## Design decisions and tradeoffs

### Why Redis plus a background worker (not a sync endpoint or `BackgroundTasks`)
- **The upstream is slow and flaky.** It runs on Render's free tier: cold starts take 30s or more and it occasionally returns 5xx. Twenty rows with retries can take minutes, far longer than a client or load balancer should hold an HTTP request open.
- **Live progress.** The worker writes after every row, so any API replica can serve status or SSE. `BackgroundTasks` keeps state inside one process, so another replica can't see it.
- **Crash recovery.** With `BackgroundTasks`, work dies with the process. With arq the job lives in Redis: if a worker dies or is redeployed, the job is re-run, and the resume logic (below) keeps that safe.
- **Scales past 20 rows.** Workers scale horizontally and the API only validates and enqueues. Validation still happens synchronously, so a bad file gets an immediate 400/422 and never reaches the queue.

### Why arq rather than Celery
The work is concurrent async HTTP. arq is asyncio-native and uses Redis, which this service already needs for
progress, so there is no second broker. Celery would mean wrapping async code in `asyncio.run` per task (or using
gevent) and running RabbitMQ or Redis plus a result backend. The tradeoff is that arq has a smaller ecosystem and
no Flower-style UI.

### The upstream is the real bottleneck, hence the global limit
Each job runs at most `PER_JOB_CONCURRENCY` (5) requests at once (an `asyncio.Semaphore`). That alone does not
protect the upstream: 10 jobs on 3 workers could still send 50 concurrent requests to a free-tier instance and get
429/5xx storms. So every HTTP attempt also takes a **lease** from `upstream:inflight` (capped at
`UPSTREAM_MAX_IN_FLIGHT`). An atomic Lua script drops expired leases and takes a slot only if the set is under the
cap.
- Leases expire after `UPSTREAM_SLOT_TTL_SECONDS`, so a crashed worker can't leak capacity, which a plain `INCR`/`DECR` counter would.
- A slot is held only during the HTTP call, never during backoff sleeps.
- **Adding workers does not raise throughput** beyond what the upstream can take; it only adds parallel jobs that wait for slots. To go faster you raise the global cap, and only if the upstream can handle it.

### Retries
Network errors, 429 (honouring a numeric `Retry-After`) and 5xx are retried with exponential backoff and jitter
(0.5s, 1s, 2s… capped at 8s). 4xx responses are **never** retried: the request is wrong, so the row is marked
failed with the upstream's message. The per-request timeout is 60s to survive cold starts.

### Partial failure policy
By default a batch is activated **only if every row succeeded**. Otherwise the job ends as `partial_failure` and the
batch stays inactive.
- **Why:** a batch is one unit of work for the user. Activating 17 of 20 hospitals publishes an incomplete directory that people may act on. The user can fix the data or call `/retry`, which re-queues only the failed rows and activates once all succeed.
- **Configurable:** set `ACTIVATE_ON_PARTIAL_FAILURE=true` when partial availability matters more than completeness, for example when each hospital is independent. Status still reports `partial_failure`, with `batch_activated: true`.
- If every row fails the job is `failed`. If activation itself fails, the job is `failed` with an `error`, and `/retry` will re-attempt activation.

### Idempotency (duplicate uploads)
- The API writes the job first, then claims `bulk:hash:{sha256}` with `SET NX`.
- If the hash already points at a job that is queued, processing or completed, the new upload deletes its own keys and returns that job with `duplicate: true`. Claiming *after* storing means the winner's metadata always exists, which makes concurrent identical uploads safe (tested with 5 in parallel).
- After a `failed` or `partial_failure` job, or once the old job has expired, re-uploading starts a fresh batch. For partial failures, `/retry` is the recommended path.

### Resumability (no duplicate hospitals)
The upstream API has no idempotency keys, so the worker prevents duplicates itself in three layers:
1. **Skip what's recorded.** Rows with a result in `bulk:{id}:results` are never sent again.
2. **Reconcile on resume.** If the job started before (crash, redelivery or `/retry`), the worker first lists `GET /hospitals/batch/{id}` and records any row that already exists upstream, matched on (name, address). CSV validation rejects duplicate name+address pairs, so the match is unambiguous. If that list call fails, the job is retried rather than risk creating rows blind.
3. **Drain on shutdown.** When arq cancels a job (SIGTERM or deploy), rows already mid-request are allowed to finish and be recorded (up to `WORKER_DRAIN_SECONDS`), and queued rows are not started. Without this, a cancelled request can still commit upstream *after* the resumed job has reconciled. A real `docker compose stop worker` test found exactly that, and it is now covered by a test. `stop_grace_period` in compose is longer than the drain window.

Infrastructure errors (such as Redis blips) raise `arq.Retry` with a growing delay. On the final try the job is
marked `failed`.

### Durability: Redis is in-memory
AOF is enabled (`appendfsync everysec`), so queued jobs and progress survive a Redis restart, losing at most about
1s of writes. All job keys expire after 24h. That keeps Redis small, but it means Redis is **not** an audit trail.
For a permanent record I would persist each finished job's metadata and per-row results to **Postgres** (in the
worker's `_finish`, or via an outbox), and keep Redis for hot progress only.

### Large files
Rows are stored in Redis because uploads are capped at 20 rows and 1 MiB. For large files I would stream the
upload to **object storage (S3)**, keep only the object key and progress counters in Redis, and have workers read
the file in chunks. That would also need `COUNT`-style progress rather than one result entry per row.

### SSE implementation
The stream polls the status in Redis every second and emits only when something changed. It also sends a
keep-alive comment every 15s for proxies. Polling reuses the status code path and works with any number of
workers. Redis pub/sub would be lower latency but adds a publish to the hot path, and messages are lost while no
one is subscribed.

---

## Configuration

Every value comes from the environment; see [`.env.example`](.env.example). The most important ones:

| Variable | Default | Meaning |
|---|---|---|
| `HOSPITAL_API_BASE_URL` | `https://hospital-directory.onrender.com` | Upstream API |
| `HOSPITAL_API_TIMEOUT_SECONDS` | `60` | Per-request timeout (cold starts) |
| `HOSPITAL_API_MAX_RETRIES` | `3` | Retries for 5xx/429/network errors |
| `MAX_CSV_ROWS` / `MAX_UPLOAD_BYTES` | `20` / `1048576` | Upload limits |
| `PER_JOB_CONCURRENCY` | `5` | Parallel upstream calls within one job |
| `UPSTREAM_MAX_IN_FLIGHT` | `10` | Global cap across all jobs and workers |
| `ACTIVATE_ON_PARTIAL_FAILURE` | `false` | Activate a batch even if some rows failed |
| `JOB_TTL_SECONDS` / `IDEMPOTENCY_TTL_SECONDS` | `86400` | Expiry of job state / duplicate detection |
| `WORKER_MAX_TRIES` / `WORKER_DRAIN_SECONDS` | `3` / `75` | Job retries / shutdown drain window |

---

## Testing

The code was built test-first. `git log` shows each `test:` (red) commit followed by its `feat:` (green) commit,
plus `refactor:` and one `fix:` commit for the bug found in the real crash test.

| Area | File | Covers |
|---|---|---|
| Upstream client | `test_hospital_api_client.py` | payload, 5xx/429/network retries, `Retry-After`, backoff caps, no retry on 4xx, limiter wraps only the HTTP call |
| CSV validation | `test_csv_validation.py` | BOM, CRLF, quoting, header rules, every file-level and row-level error, duplicates, limits |
| Progress store | `test_progress_store.py` | round trips, TTLs on every key, exact counters under 50 concurrent writes, reset, hash claim |
| Rate limiter | `test_rate_limiter.py` | cap, release, lease expiry, waiting, **global cap across 3 limiter instances** |
| Upload | `test_bulk_upload_api.py` | 202 response, stored before enqueue, 400/422 store nothing, 503 when the queue is down |
| Worker | `test_bulk_processor.py`, `test_worker.py` | activation, 5xx then success, 4xx not retried, partial failure not activated, policy flag, per-job and **cross-job global** concurrency, arq retry policy |
| Status / SSE | `test_bulk_status_api.py`, `test_bulk_events_api.py` | 404, percent, sorted results, end-to-end, event stream until `complete` |
| Idempotency / resume | `test_idempotency.py`, `test_resumability.py` | duplicate uploads (including concurrent ones), skip created rows, reconcile orphans, abort if reconciliation is impossible, drain on cancel |
| Retry / validate | `test_retry_validate_api.py` | 404/409 cases, only failed rows reset, unique arq job id, concurrent retries, full retry flow ending in activation |

---

## What I'd improve with more time

- **Ambiguous POST timeouts.** If a `ReadTimeout` happens after upstream committed the row, the client's in-loop retry can create a duplicate. The fix is to not retry ambiguous failures inline: mark the row failed, and let `/retry`'s reconcile step (which already exists) decide. Better still, add an `Idempotency-Key` header upstream.
- **Persist finished jobs to Postgres** for audit and history, with Redis as the hot cache only.
- **Stream large files to S3**, read them in chunks, and track progress by counters only.
- **Observability**: Prometheus metrics (rows/s, upstream latency, retry counts, limiter wait time), OpenTelemetry traces across API → queue → worker, and alerts on `failed` jobs.
- **Adaptive rate limiting**: shrink the global cap when the upstream returns 429/5xx and grow it back slowly (AIMD), instead of a fixed number.
- **Auth and per-tenant quotas** on the upload endpoint, plus a cleanup endpoint that calls `DELETE /hospitals/batch/{id}` to discard a partially created batch.
- **Dead-letter view** listing failed jobs, and a webhook or callback URL so clients don't need to poll.
- A **docker-compose integration test** in CI that runs against a stub upstream container.
