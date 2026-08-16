package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "task_attempt")
public class TaskAttemptEntity {

    @Id
    private UUID id;

    @Column(name = "task_instance_id", nullable = false)
    private UUID taskInstanceId;

    @Column(name = "attempt_no", nullable = false)
    private int attemptNo;

    @Column(nullable = false)
    private String state;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Column(name = "exit_code")
    private Integer exitCode;

    @Column(name = "log_tail", columnDefinition = "text")
    private String logTail;

    @Column(columnDefinition = "text")
    private String error;

    protected TaskAttemptEntity() {
    }

    public TaskAttemptEntity(UUID id, UUID taskInstanceId, int attemptNo, String state, Instant startedAt) {
        this.id = id;
        this.taskInstanceId = taskInstanceId;
        this.attemptNo = attemptNo;
        this.state = state;
        this.startedAt = startedAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTaskInstanceId() {
        return taskInstanceId;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public String getState() {
        return state;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public String getLogTail() {
        return logTail;
    }

    public String getError() {
        return error;
    }

    public void complete(String state, Instant endedAt, Integer exitCode, String logTail, String error) {
        this.state = state;
        this.endedAt = endedAt;
        this.exitCode = exitCode;
        this.logTail = logTail;
        this.error = error;
    }
}
