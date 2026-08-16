package com.buildathon.orchestrator.persistence;

import com.buildathon.orchestrator.domain.RunState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dag_run")
public class DagRunEntity {

    @Id
    private UUID id;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "dag_id", nullable = false)
    private UUID dagId;

    @Column(name = "dag_version", nullable = false)
    private int dagVersion;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RunState state;

    @Column(name = "trigger_type", nullable = false)
    private String triggerType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "trigger_payload", columnDefinition = "jsonb")
    private String triggerPayload;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected DagRunEntity() {
    }

    public DagRunEntity(UUID id, UUID dagId, int dagVersion, RunState state, String triggerType,
                        String triggerPayload, String idempotencyKey, Instant createdAt) {
        this.id = id;
        this.dagId = dagId;
        this.dagVersion = dagVersion;
        this.state = state;
        this.triggerType = triggerType;
        this.triggerPayload = triggerPayload;
        this.idempotencyKey = idempotencyKey;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getDagId() {
        return dagId;
    }

    public int getDagVersion() {
        return dagVersion;
    }

    public RunState getState() {
        return state;
    }

    public String getTriggerType() {
        return triggerType;
    }

    public String getTriggerPayload() {
        return triggerPayload;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void transition(RunState newState, Instant now) {
        com.buildathon.orchestrator.domain.RunStateMachine.requireTransition(this.state, newState);
        this.state = newState;
        if (newState == RunState.RUNNING && startedAt == null) {
            this.startedAt = now;
        }
        if (com.buildathon.orchestrator.domain.RunStateMachine.isTerminal(newState)) {
            this.endedAt = now;
        }
    }
}
