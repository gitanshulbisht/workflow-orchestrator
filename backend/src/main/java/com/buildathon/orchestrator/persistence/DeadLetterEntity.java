package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dead_letter")
public class DeadLetterEntity {

    @Id
    private UUID id;

    @Column(name = "task_instance_id", nullable = false)
    private UUID taskInstanceId;

    @Column(name = "run_id", nullable = false)
    private UUID runId;

    @Column(name = "task_name", nullable = false)
    private String taskName;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "error_payload", nullable = false, columnDefinition = "jsonb")
    private String errorPayload;

    @Column(name = "dead_lettered_at", nullable = false)
    private Instant deadLetteredAt;

    @Column(name = "replay_status", nullable = false)
    private String replayStatus;

    protected DeadLetterEntity() {
    }

    public DeadLetterEntity(UUID id, UUID taskInstanceId, UUID runId, String taskName,
                            String errorPayload, Instant deadLetteredAt) {
        this.id = id;
        this.taskInstanceId = taskInstanceId;
        this.runId = runId;
        this.taskName = taskName;
        this.errorPayload = errorPayload;
        this.deadLetteredAt = deadLetteredAt;
        this.replayStatus = "NONE";
    }

    public UUID getId() {
        return id;
    }

    public UUID getTaskInstanceId() {
        return taskInstanceId;
    }

    public UUID getRunId() {
        return runId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getErrorPayload() {
        return errorPayload;
    }

    public Instant getDeadLetteredAt() {
        return deadLetteredAt;
    }

    public String getReplayStatus() {
        return replayStatus;
    }

    public void setReplayStatus(String replayStatus) {
        this.replayStatus = replayStatus;
    }
}
