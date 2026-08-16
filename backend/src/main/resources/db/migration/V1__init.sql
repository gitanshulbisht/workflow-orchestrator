CREATE TABLE dag (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    version INT NOT NULL DEFAULT 1,
    schedule_cron VARCHAR(100),
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    is_paused BOOLEAN NOT NULL DEFAULT FALSE,
    dag_yaml TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dag_task (
    id UUID PRIMARY KEY,
    dag_id UUID NOT NULL REFERENCES dag(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    task_type VARCHAR(32) NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_retries INT NOT NULL DEFAULT 0,
    retry_delay_seconds INT NOT NULL DEFAULT 5,
    retry_backoff DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    timeout_seconds INT NOT NULL DEFAULT 300,
    is_singleton BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_dag_task_name UNIQUE (dag_id, name)
);

CREATE TABLE task_dependency (
    task_id UUID NOT NULL REFERENCES dag_task(id) ON DELETE CASCADE,
    depends_on_task_id UUID NOT NULL REFERENCES dag_task(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id),
    CONSTRAINT chk_no_self_dependency CHECK (task_id <> depends_on_task_id)
);

CREATE TABLE dag_schedule (
    dag_id UUID PRIMARY KEY REFERENCES dag(id) ON DELETE CASCADE,
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    misfire_policy VARCHAR(16) NOT NULL DEFAULT 'SKIP'
);

CREATE TABLE dag_run (
    id UUID PRIMARY KEY,
    dag_id UUID NOT NULL REFERENCES dag(id),
    dag_version INT NOT NULL,
    state VARCHAR(32) NOT NULL,
    trigger_type VARCHAR(16) NOT NULL,
    trigger_payload JSONB,
    idempotency_key VARCHAR(255),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_dag_run_idempotency_key ON dag_run (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_dag_run_dag_state ON dag_run (dag_id, state);

CREATE TABLE task_instance (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
    dag_task_id UUID NOT NULL REFERENCES dag_task(id),
    state VARCHAR(32) NOT NULL,
    attempt_no INT NOT NULL DEFAULT 0,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    claimed_by VARCHAR(255),
    heartbeat_at TIMESTAMPTZ,
    error_message TEXT,
    idempotency_key VARCHAR(255)
);
CREATE UNIQUE INDEX uq_task_instance_idempotency_key ON task_instance (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_task_instance_claim ON task_instance (state, scheduled_at);
CREATE INDEX idx_task_instance_run ON task_instance (run_id);

CREATE TABLE task_attempt (
    id UUID PRIMARY KEY,
    task_instance_id UUID NOT NULL REFERENCES task_instance(id) ON DELETE CASCADE,
    attempt_no INT NOT NULL,
    state VARCHAR(16) NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    exit_code INT,
    log_tail TEXT,
    error TEXT
);
CREATE INDEX idx_task_attempt_instance ON task_attempt (task_instance_id);

CREATE TABLE outbox_event (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    delivery_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
);
CREATE INDEX idx_outbox_pending ON outbox_event (delivery_status, id);

CREATE TABLE dead_letter (
    id UUID PRIMARY KEY,
    task_instance_id UUID NOT NULL REFERENCES task_instance(id) ON DELETE CASCADE,
    run_id UUID NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
    task_name VARCHAR(255) NOT NULL,
    error_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replay_status VARCHAR(16) NOT NULL DEFAULT 'NONE'
);

CREATE TABLE webhook (
    id UUID PRIMARY KEY,
    url VARCHAR(2048) NOT NULL,
    secret VARCHAR(255) NOT NULL,
    subscribed_events JSONB NOT NULL DEFAULT '[]'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE webhook_delivery (
    id UUID PRIMARY KEY,
    webhook_id UUID NOT NULL REFERENCES webhook(id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES outbox_event(id) ON DELETE CASCADE,
    status VARCHAR(16) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    delivered_at TIMESTAMPTZ
);

CREATE TABLE idempotency_record (
    key VARCHAR(255) PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    status_code INT NOT NULL,
    response_body JSONB,
    expires_at TIMESTAMPTZ NOT NULL
);
