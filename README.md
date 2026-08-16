# Workflow Orchestrator — an Airflow-lite DAG engine

A production-grade distributed workflow engine built for **Build-A-Thon 2026** (Theme 1: Data & Processing Pipelines). Register DAGs as YAML, trigger runs manually or on a cron schedule, and let a distributed worker pool execute tasks with retries, dead-lettering, and full observability.

**Spring Boot 4.1 (Java 21) · PostgreSQL 16 · Redis 7 · React 19**

---

## What it does

- **DAG definitions in YAML** — tasks, dependencies (parallel branches), retry policies, timeouts, cron schedules, singleton tasks
- **Distributed execution** — workers claim tasks with `FOR UPDATE SKIP LOCKED`, so multiple worker instances share one queue with at-most-once execution
- **Retries & dead-letter queue** — per-task retry policies with exponential backoff + jitter, exhausted tasks land in a DLQ with a replay API
- **Cron scheduling with leader election** — a Redisson lock guarantees exactly one scheduler instance scans schedules, even with multiple schedulers running
- **Live event stream** — every state change is written to an outbox in the same transaction, then fanned out over Redis pub/sub → SSE (dashboard) and signed webhooks (integrations)
- **Idempotency everywhere** — `Idempotency-Key` header on mutating APIs, idempotent run triggers, attempt-level idempotency keys on HTTP tasks
- **Rate limiting** — Redis-backed token bucket per API key/IP with `429 + Retry-After`
- **Crash recovery** — a reaper marks tasks whose workers stopped heartbeating as failed so they re-enter the retry path

## Judging-pattern map

Every pattern the hackathon rubric calls for is a core part of the product, not a bolt-on:

| Pattern | Where | How |
|---|---|---|
| **Idempotency** | API + domain | `Idempotency-Key` filter with a mutex insert (`idempotency_record` PK + request-hash check); run triggers dedupe via unique `idempotency_key`; HTTP tasks carry `runId:taskId:attempt` keys |
| **Outbox pattern** | `outbox/` | Every state change writes `outbox_event` in the same DB transaction; `OutboxRelay` publishes to Redis pub/sub; subscribers fan out to SSE + webhooks |
| **Retry / dead-letter** | `worker/WorkerService` | Per-task `maxRetries` + exponential backoff with full jitter (`BackoffCalculator`); attempts recorded; exhausted → `dead_letter` table + replay API |
| **Caching** | `DagService`, `StatsController` | DAG list/definition cache in Redis with **active invalidation** on register/update/pause; stats cached 10s |
| **Rate limiting** | `ratelimit/RateLimitFilter` | Redisson `RRateLimiter` per API key (fallback IP), 429 + `Retry-After` |
| **Distributed locking** | `lock/LockManager`, `scheduling/LeaderElection` | Redisson locks for scheduler leadership, singleton task serialization, per-run mutexes |

## Architecture

```
                        ┌──────────────────────────────────────────┐
   React UI (Vite) ───▶ │  API role  (Spring Boot, port 8080)      │
   Swagger UI           │  DAG CRUD · run triggers · queries       │
                        │  idempotency · rate limit · SSE          │
                        └───────────────┬──────────────────────────┘
                                        │
                 ┌──────────────────────┼──────────────────────────┐
                 ▼                      ▼                          ▼
        ┌──────────────┐      ┌──────────────────┐      ┌──────────────────┐
        │  PostgreSQL  │      │      Redis       │      │  Scheduler role  │
        │  dag, run,   │      │  leader lock     │      │  cron scanning   │
        │  instance,   │      │  rate limiters   │      │  misfire policy  │
        │  outbox,     │      │  dag cache       │      │  stale-run reaper│
        │  task queue  │      │  pub/sub         │      └──────────────────┘
        │  (SKIP       │      └───────▲──────────┘                │
        │   LOCKED)    │              │ publish                   │
        └──────▲───────┘      ┌───────┴──────────┐      ┌──────────▼────────┐
               │ claim        │  Outbox relay    │      │  Worker role(s)   │
               │              │  (api role)      │      │  claim + execute  │
               │              │  → SSE, webhooks │      │  retry, DLQ,      │
               └──────────────┴──────────────────┴──────│  heartbeats       │
                                                        └───────────────────┘
```

**One codebase, three roles.** The `ROLES` env var (`api,scheduler,worker`) turns on each role; compose runs them as separate processes, and a single Render instance runs all three (safe: locks and `SKIP LOCKED` claims make roles safe to colocate).

### Why Postgres as the queue (not Redis/Kafka)

Task claiming uses `SELECT … FOR UPDATE SKIP LOCKED` — the pattern behind real job queues. It gives us:
- **One source of truth**: queue state and business state live in the same transaction
- **No lost tasks**: rows are only visible again if a worker crashes before committing
- **No double execution**: the row lock is held until the claim commits
- Redis is used where it's genuinely better: locks, rate limiters, cache, pub/sub fan-out

### Why outbox + polling relay

State changes and event publication must not split across two systems without atomicity. The outbox table is written in the same transaction as the state change; a polling relay (crash-safe, at-least-once) publishes to Redis. The README-tradeoff: `LISTEN/NOTIFY` would lower latency; polling keeps the code simple and correct.

## Data model

```
dag(id, name UNIQUE, version, schedule_cron, timezone, is_paused, dag_yaml, …)
dag_task(id, dag_id, name, task_type, config JSONB, max_retries, retry_delay_seconds,
         retry_backoff, timeout_seconds, is_singleton)
task_dependency(task_id, depends_on_task_id)
dag_schedule(dag_id, next_run_at, last_run_at, misfire_policy)
dag_run(id, dag_id, dag_version, state, trigger_type, trigger_payload, idempotency_key UNIQUE, …)
task_instance(id, run_id, dag_task_id, state, attempt_no, scheduled_at, claimed_by,
              heartbeat_at, error_message, …)
task_attempt(id, task_instance_id, attempt_no, state, exit_code, log_tail, error)
outbox_event(id BIGSERIAL, aggregate_type, aggregate_id, event_type, payload JSONB,
             published_at, delivery_status)
dead_letter(id, task_instance_id, run_id, task_name, error_payload, replay_status)
webhook(id, url, secret, subscribed_events JSONB, is_active)
webhook_delivery(id, webhook_id, event_id, status, attempts, last_error, delivered_at)
idempotency_record(key PK, request_hash, status_code, response_body, expires_at)
```

## State machines

**Run**: `PENDING → RUNNING → SUCCESS | FAILED | CANCELLED`

**Task**:
```
PENDING ──▶ SCHEDULED ──▶ RUNNING ──▶ SUCCESS
   │            ▲            │
   │            │            └──▶ FAILED ──▶ UP_FOR_RETRY ──(backoff)──┘
   │            │                        └──▶ DEAD_LETTERED ──(replay)──▶ SCHEDULED
   ├──▶ SKIPPED (upstream failed)
   └──▶ CANCELLED (run cancelled)
```
A task becomes SCHEDULED only when all upstream tasks are SUCCESS. Failed upstream cascades SKIPPED to dependents. Transitions are enforced by a pure state machine (exhaustively unit-tested).

## API

Swagger UI: **`http://localhost:8080/swagger-ui.html`**

| Endpoint | Description |
|---|---|
| `POST /api/v1/dags` | Register a DAG (YAML/JSON body). `Idempotency-Key` honored |
| `GET /api/v1/dags` / `GET /api/v1/dags/{id}` | List / detail with task graph |
| `PATCH /api/v1/dags/{id}` | Pause/resume (JSON) or new definition (YAML, bumps version) |
| `POST /api/v1/dags/{id}/runs` | Trigger a run. `Idempotency-Key` honored |
| `POST /api/v1/dags/{id}/runs/{runId}/cancel` | Cancel a running run |
| `GET /api/v1/runs` · `GET /api/v1/runs/{runId}` · `GET /api/v1/runs/{runId}/tasks` | Run listing / timeline |
| `GET /api/v1/dead-letters` · `POST /api/v1/dead-letters/{id}/replay` | DLQ + replay |
| `GET/POST /api/v1/webhooks` · `PATCH /api/v1/webhooks/{id}` | Webhook subscriptions |
| `GET /api/v1/stats` | Dashboard stats (cached 10s) |
| `GET /api/v1/events/stream` | SSE stream of outbox events |

Errors use RFC 7807 problem details. Rate-limited responses carry `Retry-After`.

## Running it

### Local (docker compose)

```bash
docker compose up --build
```

Brings up Postgres, Redis, the API (`:8080`), two schedulers (one leader — watch the logs!), and two workers.

- Dashboard: http://localhost:8080
- Swagger: http://localhost:8080/swagger-ui.html
- Health: http://localhost:8080/actuator/health

### Development (backend + frontend separately)

```bash
# terminal 1 — infra
docker compose up -d postgres redis

# terminal 2 — backend (needs Java 21 + Maven)
cd backend
./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# terminal 3 — frontend (Vite dev server with /api proxy)
cd frontend
npm install --include=dev
npm run dev
```

### Tests

```bash
cd backend
./mvnw test
```

76 tests: pure-domain unit tests (state machines, validation, backoff) plus Testcontainers integration tests that boot real Postgres + Redis — including a concurrency test proving two parallel transactions never claim the same task.

## Demo script (2–5 min video)

1. Register a 5-task DAG (parallel fetch → process → notify) in the UI or via Swagger
2. Trigger a run — watch the live timeline update via SSE
3. Re-trigger with the same `Idempotency-Key` — same run returned, no duplicate
4. Show retries: a `fail` task with `maxRetries: 2` fails, backs off, succeeds on the last attempt
5. Show the DLQ: a task with no retries left dead-letters; hit replay and watch the run finish
6. `docker compose logs scheduler-2` — the second scheduler never scans (leader election)
7. Burst the API — 429 with `Retry-After`
8. Point a webhook at webhook.site — signed events arrive in order (outbox)

## Deploying to Render

1. Push this repo to GitHub
2. In Render: **New → Blueprint**, select the repo — `render.yaml` creates the web service, Postgres, and Redis
3. Set no secrets (dev defaults work); after deploy, open `/actuator/health` to verify

## Project layout

```
backend/     Spring Boot application (api + scheduler + worker roles)
frontend/    Vite + React + TypeScript dashboard
docs/        Architecture write-up and diagrams
```

## Trade-offs & future work

- **Outbox relay polls** — `LISTEN/NOTIFY` would cut latency to near-zero; polling is simpler and crash-safe
- **Workers execute in-process** — the `TaskExecutor` interface is the seam for a future sandbox/container-isolated worker
- **Single Postgres** — the SKIP LOCKED queue scales surprisingly far; a partition by dag_id is the documented growth path
- **Webhook delivery** — in-process dispatcher with retries; a dedicated delivery service (with its own DLQ) is the production next step
