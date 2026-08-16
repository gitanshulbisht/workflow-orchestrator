package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dag")
public class DagEntity {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private String name;

    private String description;

    @Column(nullable = false)
    private int version;

    private String scheduleCron;

    @Column(nullable = false)
    private String timezone;

    @Column(name = "is_paused", nullable = false)
    private boolean paused;

    @Column(nullable = false, columnDefinition = "text")
    private String dagYaml;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    protected DagEntity() {
    }

    public DagEntity(UUID id, String name, String description, int version, String scheduleCron,
                     String timezone, boolean paused, String dagYaml, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.version = version;
        this.scheduleCron = scheduleCron;
        this.timezone = timezone;
        this.paused = paused;
        this.dagYaml = dagYaml;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getVersion() {
        return version;
    }

    public String getScheduleCron() {
        return scheduleCron;
    }

    public String getTimezone() {
        return timezone;
    }

    public boolean isPaused() {
        return paused;
    }

    public String getDagYaml() {
        return dagYaml;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void update(String description, int newVersion, String scheduleCron, String timezone,
                       boolean paused, String dagYaml, Instant updatedAt) {
        this.description = description;
        this.version = newVersion;
        this.scheduleCron = scheduleCron;
        this.timezone = timezone;
        this.paused = paused;
        this.dagYaml = dagYaml;
        this.updatedAt = updatedAt;
    }

    public void setPaused(boolean paused, Instant updatedAt) {
        this.paused = paused;
        this.updatedAt = updatedAt;
    }
}
