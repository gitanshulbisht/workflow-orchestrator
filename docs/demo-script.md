# Demo Video Script (2–5 minutes)

**Setup before recording:** `docker compose up --build` running, dashboard open at
http://localhost:8080, Swagger open in a second tab.

## 0. Intro (15s)
"This is a distributed DAG workflow engine — an Airflow-lite. Spring Boot, Postgres,
Redis. One codebase, three roles: API, scheduler, and workers. Let me walk through
the judging patterns live."

## 1. Register + run a DAG (45s)
- Dashboard → paste the sample YAML (parallel fetch → process → notify + a flaky task)
- Register → shows in the DAG table with tasks and dependency graph
- Trigger → switch to the run page: watch task states flip live (SSE)
- "The timeline you're watching is streamed over SSE from the outbox — every state
  change is written to Postgres in the same transaction, then fanned out."

## 2. Idempotency (20s)
- Re-trigger with the same Idempotency-Key header (Swagger)
- Same run ID comes back; no duplicate rows
- "Three levels: API filter with hash validation, unique key on the run, and
  attempt-level keys on HTTP tasks."

## 3. Retry + backoff (30s)
- The `flaky` task (maxRetries=2) fails → UP_FOR_RETRY → visible backoff (scheduledAt)
- "Retries are data, not sleeps — the next attempt time is computed with exponential
  backoff and full jitter, any worker can pick it up."

## 4. Dead-letter + replay (45s)
- `flaky` exhausts retries → DEAD_LETTERED → appears on the Dead Letters page
- Downstream task shows SKIPPED (cascade)
- Hit Replay → task goes back to SCHEDULED → run finishes FAILED → final state
- "Dead letters are first-class: queryable, replayable, with the error payload."

## 5. Leader election (30s)
- `docker compose ps` shows two scheduler containers
- `docker compose logs scheduler-2 | grep leader` — contends for the lock but only one holds it per scan cycle
- "A Redisson lock guarantees exactly one cron scanner. Kill the leader and the
  other takes over within one tick."

## 6. Rate limiting (20s)
- `seq 25 | xargs -P25 curl ... -H "X-API-Key: demo"` → 20×200, 5×429 with Retry-After
- "Redis-backed token bucket, keyed by API key, falling back to IP."

## 7. Outbox → webhook (30s)
- Register a webhook pointing at webhook.site
- Trigger a run → webhook.site shows signed events arriving in order
- "HMAC-SHA256 signed, retried up to 3 times, with delivery tracking."

## 8. Close (15s)
- Point at Swagger for the full API surface
- "Postgres is the queue — SKIP LOCKED claims give at-most-once execution with zero
  message loss. Thanks!"

## Talking points if asked
- Why Postgres as the queue: transactional consistency, SKIP LOCKED, crash-safe claims
- Why outbox + polling relay: atomicity between state and events; LISTEN/NOTIFY as the upgrade path
- Crash recovery: reaper marks stale heartbeats FAILED → retry path
- 79 tests: state machines, validation, backoff, webhook dispatch, and Testcontainers concurrency tests
