# Interactive Architecture Diagram Design Spec

**Date**: 2026-08-22  
**Status**: Approved  
**Target File(s)**: `README.md`, `docs/architecture.md`, `docs/architecture.svg`, `docs/architecture.html`

---

## 1. Goal & Context

The current `README.md` and `docs/architecture.md` use static ASCII art diagrams to represent the distributed workflow engine's architecture. While descriptive, ASCII art lacks interactive capability, modern visual aesthetics, node-level code deep links, and dynamic simulation of key features (such as `SELECT FOR UPDATE SKIP LOCKED` task claiming, Redisson leader locks, outbox event relaying, and backoff retries).

This specification outlines the addition of three complementary interactive diagram layers:
1. **GitHub Native Mermaid.js Diagram** directly in `README.md` with interactive code links.
2. **Interactive SVG Architecture Illustration** saved to `docs/architecture.svg` and embedded in `README.md`.
3. **Interactive HTML Architecture Explorer App** created in `docs/architecture.html` providing animated flow simulations and state machine exploration.

---

## 2. README.md Diagram Enhancements

### 2.1 Native Mermaid.js Flowchart
The ASCII diagram in `README.md` under `## Architecture` will be replaced/enhanced with a styled Mermaid.js flowchart (` ```mermaid `):
- **Subgraphs**: API Role, Scheduler Role, Worker Pool, Postgres Database (with Outbox & DLQ tables), and Redis.
- **Node Styling**: Custom CSS class definitions for component roles, storage layers, and pub/sub channels.
- **Click Directives**: GitHub-native `click` actions linking key nodes directly to their primary Java implementation files (e.g. `WorkerService.java`, `OutboxRelayRunner.java`, `LeaderElection.java`).

### 2.2 Interactive Vector SVG Diagram (`docs/architecture.svg`)
A high-resolution, vector-drawn SVG diagram will be saved in `docs/architecture.svg` and embedded in `README.md` using HTML `<img>` and `<object>` markup:
- **Visual Design**: Sleek dark mode palette matching the React dashboard aesthetics.
- **Hover & Tooltip Effects**: Hovering over system nodes highlights component boundaries and reveals metadata (e.g., ports, concurrency parameters, table names).
- **Interactive Hotspots**: Clickable SVG links pointing to source files and documentation anchors.

---

## 3. Standalone Interactive HTML Architecture Explorer (`docs/architecture.html`)

A single-file HTML/CSS/JS web application located at `docs/architecture.html` will provide an interactive browser-based experience for demonstrating the workflow orchestrator.

### 3.1 Components & Architecture Topology
- Visual map representing the 3 logical roles (`api`, `scheduler`, `worker`) and 2 storage engines (`PostgreSQL`, `Redis`).
- Clicking any node opens an inspection panel detailing:
  - Role responsibility and startup flag (`ROLES=...`)
  - Concurrency guarantees enforced (e.g., `SKIP LOCKED` at-most-once execution, Redisson lock)
  - Key database tables modified
  - Clickable links to relevant Java classes in the repository

### 3.2 Animated Data Flow Simulator
Interactive control buttons allowing the user to trigger and step through real workflow scenarios:
1. **Trigger Run (API → DB → Outbox → Redis → SSE/Webhooks)**: Animates an event packet moving through the outbox transaction to Redis pub/sub and fan-out.
2. **Task Claim & Execution (`SKIP LOCKED`)**: Animates a worker acquiring a `SCHEDULED` task row and executing it outside the transaction with heartbeat refreshes.
3. **Task Failure & Exponential Backoff Retry**: Animates a task failing, computing `scheduled_at = now + jitter`, and moving to `UP_FOR_RETRY`.
4. **Leader Election Cycle**: Animates Redisson `tryLock` tick where Scheduler 1 retains leadership while Scheduler 2 waits.

### 3.3 Task & Run State Machine Visualizer
- Interactive state graph showing valid transitions for `TaskState` (`PENDING` → `SCHEDULED` → `RUNNING` → `SUCCESS` / `FAILED` → `UP_FOR_RETRY` → `DEAD_LETTERED` / `SKIPPED` / `CANCELLED`).
- Clicking a state highlights incoming/outgoing transitions and shows the triggering mechanism.

---

## 4. Proposed Changes & Files

| Action | Path | Purpose |
|---|---|---|
| **[MODIFY]** | [README.md](file:///Users/anshulbisht/Build-A-thon/workflow-orchestrator/README.md) | Update Architecture section with interactive SVG embedding and Mermaid flowchart |
| **[MODIFY]** | [docs/architecture.md](file:///Users/anshulbisht/Build-A-thon/workflow-orchestrator/docs/architecture.md) | Add links to SVG and interactive HTML visualizer |
| **[NEW]** | [docs/architecture.svg](file:///Users/anshulbisht/Build-A-thon/workflow-orchestrator/docs/architecture.svg) | Interactive SVG architecture illustration with tooltips & hotspots |
| **[NEW]** | [docs/architecture.html](file:///Users/anshulbisht/Build-A-thon/workflow-orchestrator/docs/architecture.html) | Standalone interactive HTML architecture explorer & data flow simulator |

---

## 5. Verification & Testing

- **GitHub Markdown Verification**: Ensure the Mermaid diagram syntax is valid and renders correctly without errors.
- **Browser Testing**: Verify `docs/architecture.html` and `docs/architecture.svg` in Chrome/Safari/Firefox for responsive layout, interactive clicks, animations, and zero console errors.
- **Link Integrity Check**: Validate all GitHub links in SVG, Mermaid, and HTML lead to existing files in the repository.
