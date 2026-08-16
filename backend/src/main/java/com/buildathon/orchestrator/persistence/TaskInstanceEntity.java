package com.buildathon.orchestrator.persistence;

import com.buildathon.orchestrator.domain.TaskState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "task_instance")
public class TaskInstanceEntity {

    @Id
    private UUID id;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "run_id", nullable = false)
    private UUID runId;

    @Column(name = "dag_task_id", nullable = false)
    private UUID dagTaskId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TaskState state;

    @Column(name = "attempt_no", nullable = false)
    private int attemptNo;

    @Column(name = "queued_at", nullable = false)
    private Instant queuedAt;

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Column(name = "claimed_by")
    private String claimedBy;

    @Column(name = "heartbeat_at")
    private Instant heartbeatAt;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    protected TaskInstanceEntity() {
    }

    public TaskInstanceEntity(UUID id, UUID runId, UUID dagTaskId, TaskState state, Instant queuedAt,
                              Instant scheduledAt, String idempotencyKey) {
        this.id = id;
        this.runId = runId;
        this.dagTaskId = dagTaskId;
        this.state = state;
        this.queuedAt = queuedAt;
        this.scheduledAt = scheduledAt;
        this.idempotencyKey = idempotencyKey;
    }

    public UUID getId() {
        return id;
    }

    public UUID getRunId() {
        return runId;
    }

    public UUID getDagTaskId() {
        return dagTaskId;
    }

    public TaskState getState() {
        return state;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public Instant getQueuedAt() {
        return queuedAt;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public String getClaimedBy() {
        return claimedBy;
    }

    public Instant getHeartbeatAt() {
        return heartbeatAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void transition(TaskState newState, Instant now) {
        com.buildathon.orchestrator.domain.TaskStateMachine.requireTransition(this.state, newState);
        this.state = newState;
        switch (newState) {
            case SCHEDULED -> {
                if (scheduledAt == null || scheduledAt.isBefore(now)) {
                    this.scheduledAt = now;
                }
                this.endedAt = null;
            }
            case RUNNING -> {
                this.startedAt = now;
                this.endedAt = null;
                this.claimedBy = null;
            }
            case UP_FOR_RETRY -> {
                this.endedAt = null;
            }
            case SUCCESS, FAILED, DEAD_LETTERED, SKIPPED, CANCELLED -> {
                this.endedAt = now;
                this.claimedBy = null;
            }
            default -> {
            }
        }
    }

    public void claim(String workerId, Instant now) {
        com.buildathon.orchestrator.domain.TaskStateMachine.requireTransition(this.state, TaskState.RUNNING);
        this.state = TaskState.RUNNING;
        this.claimedBy = workerId;
        this.startedAt = now;
        this.heartbeatAt = now;
        this.endedAt = null;
    }

    public void heartbeat(Instant now) {
        this.heartbeatAt = now;
    }

    public void markError(String message) {
        this.errorMessage = message;
    }

    public void scheduleRetry(int nextAttemptNo, Instant nextAttemptAt) {
        this.attemptNo = nextAttemptNo;
        this.scheduledAt = nextAttemptAt;
        this.claimedBy = null;
        this.heartbeatAt = null;
    }
}
