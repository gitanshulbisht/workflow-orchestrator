package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dag_schedule")
public class DagScheduleEntity {

    @Id
    @Column(name = "dag_id")
    private UUID dagId;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(name = "last_run_at")
    private Instant lastRunAt;

    @Column(name = "misfire_policy", nullable = false)
    private String misfirePolicy;

    protected DagScheduleEntity() {
    }

    public DagScheduleEntity(UUID dagId, Instant nextRunAt, Instant lastRunAt, String misfirePolicy) {
        this.dagId = dagId;
        this.nextRunAt = nextRunAt;
        this.lastRunAt = lastRunAt;
        this.misfirePolicy = misfirePolicy;
    }

    public UUID getDagId() {
        return dagId;
    }

    public Instant getNextRunAt() {
        return nextRunAt;
    }

    public Instant getLastRunAt() {
        return lastRunAt;
    }

    public String getMisfirePolicy() {
        return misfirePolicy;
    }

    public void setNextRunAt(Instant nextRunAt) {
        this.nextRunAt = nextRunAt;
    }

    public void setLastRunAt(Instant lastRunAt) {
        this.lastRunAt = lastRunAt;
    }
}
