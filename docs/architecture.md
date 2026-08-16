# Architecture

## Roles and processes

The codebase produces one artifact that starts one of three roles based on `ROLES`:

- **api** — HTTP surface (DAG CRUD, run triggers, queries), outbox relay, SSE endpoint, webhook dispatch
- **scheduler** — cron scanning, misfire handling, stale-task reaping (leader-elected)
- **worker** — claims SCHEDULED tasks with SKIP LOCKED, executes, retries, dead-letters, heartbeats

In docker compose these are separate processes; on Render one instance runs all three roles. That's safe because:
- Worker claims are mutually exclusive via `FOR UPDATE SKIP LOCKED`
- Scheduler scans are serialized by the Redisson leader lock
- Outbox relay idempotency: at-least-once publish, marking as PUBLISHED after the fact

## Core flows

### 1. DAG registration

```
POST /api/v1/dags (YAML)
  → DagParser.parse → DagValidator.validate (cycles, names, cron, bounds)
  → transaction { insert dag, dag_task*, task_dependency* }
  → invalidate Redis DAG cache
```

### 2. Run triggering

```
POST /api/v1/dags/{id}/runs
  → (Idempotency-Key → dag_run.idempotency_key UNIQUE, duplicate returns original)
  → transaction {
        insert dag_run (PENDING)
        insert task_instance per task (roots → SCHEDULED, others → PENDING)
        write outbox events (DAG_RUN_CREATED, TASK_INSTANCE_SCHEDULED)
     }
  → outbox relay → Redis pub/sub → SSE + webhooks
```

### 3. Task execution & retry

```
worker poll → claimNextDue (SELECT … FOR UPDATE SKIP LOCKED) → claim (RUNNING + heartbeat)
  → executor (bash/http/delay/fail) with timeout
  → record attempt (SUCCESS/FAILED, exit code, log tail)
  → SUCCESS: propagate downstream (PENDING → SCHEDULED when all upstream SUCCESS;
              SKIPPED when any upstream terminal-failed); finalize run if complete
  → FAILED with attempts left: FAILED → UP_FOR_RETRY, scheduled_at = now + backoff(jitter)
  → FAILED exhausted: FAILED → DEAD_LETTERED + dead_letter row
```

Retry timing is **data, not sleeps** — workers never block waiting for retries; the retry window is the computed `scheduled_at`, and any worker can pick it up when due.

### 4. Cron scheduling with leader election

```
scheduler tick (every 2s):
  tryAcquire("scheduler-leader")  ← Redisson lock, zero-wait
  if leader:
    scan due dag_schedule rows
    trigger run (SKIP → advance to next future fire; BACKFILL → one run per missed fire)
    reap tasks whose heartbeat_at < now - staleHeartbeatMs  (FAILED → retry path)
  release lock
```

### 5. Dead-letter replay

```
POST /api/v1/dead-letters/{id}/replay
  → task DEAD_LETTERED → SCHEDULED (scheduled_at = now)
  → dead_letter.replay_status = REPLAYED (idempotent)
  → worker picks it up like any scheduled task
```

### 6. Outbox → fan-out

```
state change tx writes outbox_event(PENDING)
OutboxRelay (1s poll): PENDING → publish(Redis CHANNEL) → mark PUBLISHED
Redis subscribers:
  EventFanOut → SSE broadcaster → browser dashboard
  WebhookDispatcher → signed HTTP POST (HMAC-SHA256) → delivery records + retries
```

## Concurrency guarantees

| Guarantee | Mechanism |
|---|---|
| At-most-once task execution | `FOR UPDATE SKIP LOCKED` + claim transaction |
| Single scheduler | Redisson leader lock (tryLock with zero wait each tick) |
| Singleton tasks never parallel | Redisson lock per `dagId:taskId`; busy → reschedule |
| No lost events | Outbox row in same tx as state change |
| No duplicate runs for a trigger | `dag_run.idempotency_key` UNIQUE + API-level idempotency filter |
| No duplicate API effects | `idempotency_record` PK as mutex (insert-first), request-hash validation |
| Run finalization exactly once | Terminal-state check under the propagating transaction |

## Failure recovery

- **Worker crash mid-task**: heartbeat goes stale → reaper marks FAILED → retry path (or DLQ if exhausted)
- **Scheduler crash**: lock expires; another scheduler instance takes over within one tick
- **Redis down**: API still serves (cache/locks degrade to miss/no-op); outbox events accumulate in Postgres until Redis returns
- **Postgres down**: everything waits; nothing is lost because all state is transactional

## Tech choices

| Concern | Choice | Why |
|---|---|---|
| Queue | Postgres SKIP LOCKED | Transactional consistency with business state; zero message loss |
| Locks / rate limits | Redisson (raw client) | Redis-backed fairness, watchdogs, TTL expiry |
| Cache / pub-sub | Spring Data Redis (Lettuce) | Standard integration with Boot 4 |
| Cron parsing | cron-utils (Quartz format) | Battle-tested, correct across timezones |
| API docs | springdoc-openapi | Swagger UI out of the box |
| Tests | JUnit 5 + AssertJ + Awaitility + Testcontainers | Real Postgres/Redis in CI, not mocks |
