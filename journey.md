# Journey — Workflow Orchestrator

A chronological log of every change made to this application, the problems encountered at each stage, and how they were resolved.

---

## Stage 1 — Repository setup

**What happened**

The project lived as a local folder (`workflow-orchestrator/`) with a Spring Boot backend, a React frontend, Docker compose setup, and deployment configs. It was already a git repo but had zero commits.

**Problems faced**

- The top-level `Build-A-thon/` directory was not a git repo; the app was nested inside it.
- The app folder had no commits yet, so there was no history to build on.
- A secret scan was needed before pushing anything public.

**How it was resolved**

- Initialized the commit inside `workflow-orchestrator/` (the app's own repo), leaving the parent directory alone.
- Scanned for `.env` files and hardcoded secrets — none found; the existing `.gitignore` covered `node_modules`, `dist`, `target`, and `.DS_Store`.
- Created the GitHub repo `gitanshulbisht/workflow-orchestrator`, committed all 136 files, and pushed.

**Result:** repo live at `https://github.com/gitanshulbisht/workflow-orchestrator`.

---

## Stage 2 — Local test run

**What happened**

Started the full stack with `docker compose up --build` to test the application end to end.

**Problems faced**

- Docker daemon wasn't running (user started it manually).
- A local Homebrew Postgres was holding port 5432, which the compose file's Postgres container needs.
- No Java runtime was linked on the host for running the Maven test suite locally.

**How it was resolved**

- Stopped the Homebrew Postgres with `brew services stop postgresql@18` to free 5432.
- Used Homebrew's `openjdk@21` (installed but not on PATH) via `JAVA_HOME` export for compiling and running tests.

**Result:** all 7 containers healthy, dashboard up at `http://localhost:8080`.

---

## Stage 3 — Feature smoke tests + bug 1: webhooks never fired

**What happened**

Tested every feature end to end: DAG registration, happy-path run, retries with backoff, dead-letter + replay, idempotency, rate limiting, cancel, cron scheduling, SSE stream, and webhooks.

**Problems faced**

- **Bug 1:** Registered webhooks never received any events. The `webhook_delivery` table stayed empty even though 90+ outbox events existed.
  - Root cause: `WebhookDispatcher.dispatchPending()` existed but was never called by anything. The outbox relay published events to Redis but never invoked the webhook dispatcher. There was also no dedup, so wiring it in naively could double-deliver.

**How it was resolved**

- Wired `WebhookDispatcher.dispatchPending()` into `OutboxRelayRunner` (the api-role scheduled loop) right after publishing.
- Added an `existsByWebhookIdAndEventId` guard so a crash/retry can't create duplicate deliveries.
- Added a `DISPATCHED` status to outbox events so processed events are marked and skipped on the next sweep.
- Added `WebhookDispatcherIntegrationTest` — spins up a real `HttpServer`, registers a webhook, dispatches, and asserts exactly one signed delivery arrives and a second dispatch is a no-op.

**Result:** 131 signed deliveries observed live; regression test added (79 tests total).

---

## Stage 4 — Bug 2: DAG updates crashed with FK violations

**What happened**

While smoke-testing `PATCH /api/v1/dags/{id}` with a new YAML definition, the API returned HTTP 500.

**Problems faced**

- Root cause: `DagService.updateDefinition()` deleted the old `dag_task` rows and re-inserted new ones. But historical `task_instance` rows reference `dag_task(id)` via a foreign key, so the delete failed with `update or delete on table "dag_task" violates foreign key constraint "task_instance_dag_task_id_fkey"`.

**How it was resolved**

- Added a `version` column to `dag_task` (migration `V3__dag_task_version.sql`), stamped with the DAG version it belongs to.
- `updateDefinition()` no longer deletes anything — it bumps the DAG version and inserts a fresh task row set stamped with the new version. Old rows remain for historical runs.
- `RunService.trigger()` and `DagService.getTasks()/getDependencies()` resolve only the current version's tasks.
- Unique constraint changed from `(dag_id, name)` to `(dag_id, name, version)` so repeated versions of the same task name coexist.
- Added regression test `patchWithNewYamlSucceedsWhenHistoricalTaskInstancesExist`.

**Result:** DAG updates bump versions cleanly; old runs keep their v1 task references, new runs use v2 tasks.

---

## Stage 5 — Bug 3: cancelling a run with a RUNNING task failed

**What happened**

Testing the cancel endpoint on a run whose task had already been claimed and was RUNNING returned 409.

**Problems faced**

- The task state machine had no `RUNNING → CANCELLED` transition — only `PENDING`/`SCHEDULED` tasks could be cancelled.
- Even after allowing that transition, the cancel loop iterated task rows and called `instance.transition(...)`, which hit optimistic-lock conflicts when the worker updated the same row concurrently.

**How it was resolved**

- Added `RUNNING → CANCELLED` to `TaskStateMachine` (and its exhaustive unit test).
- Replaced the per-row cancel loop with a single bulk `UPDATE ... WHERE run_id = ? AND state IN (PENDING, SCHEDULED, RUNNING, UP_FOR_RETRY)` — race-tolerant by construction.
- Added regression test `cancellingRunWithRunningTaskCancelsIt`.

**Result:** cancel now works even when a task is mid-flight.

---

## Stage 6 — Docs sync

**What happened**

Updated README and demo script to match the code changes.

**Problems faced**

- Stale docs: the data-model listing didn't include the new `dag_task.version` column, the task state diagram missed `RUNNING → CANCELLED`, the test count said 76 instead of 79, and the demo script claimed a `fail` task "succeeds on the last attempt" (impossible — `fail` always fails) and that the second scheduler "never acquires" the lock (it does contend per scan cycle).

**How it was resolved**

- Corrected the schema, diagram, test count, and both demo-script claims.

---

## Stage 7 — Render deployment

**What happened**

Deployed the application to Render into the user's existing **Build-A-Thon** project, reusing it as requested.

**Problems faced**

- **Bug 4:** `render.yaml` referenced a Redis service (`orchestrator-redis`) for env vars but never defined it — blueprint validation would have failed. Fixed by adding the Redis service with an open IP allow list.
- The Render API has no blueprint-creation endpoint, so the deployment was done with the Render CLI + REST API: created Postgres (`orchestrator-db`, free), Redis (`orchestrator-redis`, free), and the web service (`workflow-orchestrator`, Docker, free plan).
- The free plan required no payment but the CLI's `starter`/`standard` plans returned 402; the existing account service used plan `free`, which worked.
- First deploys failed with `password authentication failed for user "orchestrator"` — the env-var PUT replaced the whole set, wiping `DB_USER`/`DB_PASSWORD`. Redeployed with all 7 env vars in one PUT.
- The external DB hostname worked for health but the canonical internal hostname is used for service-to-service traffic (same Render network).

**Result:** live at `https://workflow-orchestrator-yij6.onrender.com`.

---

## Stage 8 — Bug 5: Events page showed "waiting for events" forever

**What happened**

While recording the demo video, the Events page never populated even though `curl` against the SSE endpoint showed events flowing.

**Problems faced**

- The backend broadcasts SSE with **named** events (`event: DAG_RUN_CREATED`, …), but the frontend only registered a generic `message` listener — which never fires for named events.
- The backend fan-out also stripped the envelope before broadcasting, sending only `payload`, so even if the listener had fired, the UI wouldn't have had `eventType`/`aggregateType`/`id`/`createdAt` to render.
- A third latent bug: the frontend skipped any data starting with `{` (meant for the `connected` ping), but real event envelopes are JSON objects too.

**How it was resolved**

- `useEventStream.ts` now registers listeners for every named event type the backend emits, parses the full envelope, and ignores only messages without an `eventType` (the `connected` ping).
- `EventFanOut` now forwards the full envelope instead of just the payload.

**Result:** Events page streams live events; verified with a 14-row table during a single run.

---

## Stage 9 — Demo video

**What happened**

Recorded and composed a 100-second demo video with text overlays explaining each feature, committed as `demo-video.mp4`.

**Problems faced**

- ffmpeg from Homebrew lacked the `drawtext` filter needed for text overlays — had to install `ffmpeg-full`.
- `agent-browser` recordings landed in the repo root instead of the scratch dir — moved them.
- A "terminal demo" segment recorded black frames because injecting DOM into a React page gets wiped on re-render — re-did it as a standalone static HTML page with typed-out terminal lines.
- Early segments were too long (one was 307s from a forgotten recording) — trimmed to the useful parts before assembly.

**How it was resolved**

- Segments recorded with `agent-browser record` against the local app, trimmed with ffmpeg, overlaid with `drawtext` narration, concatenated, and frame-checked with vision before committing.

---

## Stage 10 — Bug 6: long-running tasks got reaped as "stale"

**What happened**

During live deployment core checks, a 90-second `delay` task was marked FAILED with "Worker heartbeat lost (stale) — recovered by reaper" after ~30 seconds, even though the worker was alive.

**Problems faced**

- Heartbeats were written only once, at task start. The reaper kills any RUNNING task whose heartbeat is older than 30s — so any legitimate task running longer than 30s was doomed.

**How it was resolved**

- Added `TaskInstanceRepository.refreshHeartbeat(id, now)` — a conditional UPDATE that only touches RUNNING rows — and a daemon thread in `WorkerService` that refreshes the heartbeat every `heartbeatIntervalMs` while the executor runs, stopping when the task finishes.

**Result:** commit `644125d`. But this exposed the next bug…

---

## Stage 11 — Bug 7: heartbeats deadlocked behind the worker's own transaction

**What happened**

After deploying the heartbeat fix, tasks were still being reaped on Render.

**Problems faced**

- The whole task execution (executor run included) happened inside **one long transaction** that held the `task_instance` row lock. The heartbeater's UPDATE waited on that lock until the task finished — so the heartbeat never landed in time, and the reaper killed the task anyway. The logs showed `StaleObjectStateException` from the heartbeater's blocked UPDATE once the transaction finally released.

**How it was resolved**

- Restructured `WorkerService` execution into three short transactions:
  1. **Prepare** — create the attempt row, stamp the initial heartbeat, commit.
  2. **Execute** — run the executor **outside any transaction** so heartbeats can update the row freely.
  3. **Finalize** — record the attempt outcome, transition state, write outbox events, propagate downstream, commit.
- `executeWithSingletonGuard` now takes the instance id and loads a fresh entity for the executor.

**Result:** verified locally and on Render that a 90s task survives past the 30s reaper threshold and completes SUCCESS; cancel still works mid-flight.

---

## Stage 12 — Bug 8: Redis connection exhaustion on Render

**What happened**

A deploy failed with `ERR max number of clients reached` from Redis.

**Problems faced**

- Render's free Redis tier allows only a handful of client connections. Redisson's default pool (~24 connections) plus Spring Data Redis/Lettuce's default pool exhausted it during startup.

**How it was resolved**

- Capped both pools: Redisson `connectionPoolSize=4` / `minimumIdleSize=1`, and Lettuce `max-active=4` / `max-idle=2` / `min-idle=1` in `application.yml`.

**Result:** deploy succeeded and stayed stable under load.

---

## Stage 13 — Final live core checks

**What happened**

Ran a systematic core-functionality pass against the live Render deployment.

**Verified working on production:**

- Health UP, dashboard 200, Swagger UI + API docs serving
- Happy-path DAG: PENDING → SUCCESS with tasks in dependency order
- Retries (3 attempts), DEAD_LETTERED with error payload, downstream SKIPPED, run FAILED, replay → REPLAYED
- Idempotency: same key → same run ID; conflicting body → 422
- Rate limiting: parallel bursts → 429 with `Retry-After`
- Cancel mid-flight: RUNNING task → CANCELLED cleanly
- Long task alive past 30s (heartbeat fix), then cancelled
- SSE stream: 11 named events captured during one run
- Webhook registration: 201 with active subscription
- Stats: accurate live counts

---

## Final state

- **GitHub:** `gitanshulbisht/workflow-orchestrator` — 11 commits, all changes pushed
- **Live deployment:** `https://workflow-orchestrator-yij6.onrender.com` (Render, Build-A-Thon project, free plan)
- **Tests:** 79 passing (unit + Testcontainers integration)
- **Deliverables:** README, docs/, demo-script.md, demo-video.mp4, journey.md

### Bug ledger

| # | Bug | Stage | Root cause | Fix |
|---|-----|-------|------------|-----|
| 1 | Webhooks never delivered | 3 | `dispatchPending()` never called | Wire into relay + dedup + DISPATCHED status |
| 2 | DAG update → 500 FK violation | 4 | Deleting `dag_task` rows referenced by history | Version `dag_task` rows, keep history |
| 3 | Cancel on RUNNING task → 409 | 5 | Missing state transition + per-row race | Allow `RUNNING→CANCELLED` + bulk UPDATE |
| 4 | Blueprint invalid (Redis undefined) | 7 | `render.yaml` referenced undefined service | Define Redis service in blueprint |
| 5 | Events page always empty | 8 | Named SSE events vs `message` listener + envelope stripping | Listen per event type, forward full envelope |
| 6 | Long tasks reaped as stale | 10 | Heartbeat written only once at start | Heartbeat refresher thread |
| 7 | Heartbeat refresh blocked | 11 | Executor ran inside row-lock-holding transaction | Execute outside transaction |
| 8 | Redis max clients reached | 12 | Default pools exceed free-tier connection limit | Cap Redisson + Lettuce pools |
