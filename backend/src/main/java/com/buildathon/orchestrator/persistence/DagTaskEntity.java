package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.util.UUID;

@Entity
@Table(name = "dag_task")
public class DagTaskEntity {

    @Id
    private UUID id;

    @Column(name = "dag_id", nullable = false)
    private UUID dagId;

    @Column(nullable = false)
    private String name;

    @Column(name = "task_type", nullable = false)
    private String taskType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String config;

    @Column(nullable = false)
    private int maxRetries;

    @Column(nullable = false)
    private int retryDelaySeconds;

    @Column(nullable = false)
    private double retryBackoff;

    @Column(nullable = false)
    private int timeoutSeconds;

    @Column(nullable = false)
    private int version;

    @Column(name = "is_singleton", nullable = false)
    private boolean singleton;

    protected DagTaskEntity() {
    }

    public DagTaskEntity(UUID id, UUID dagId, String name, String taskType, String config,
                         int maxRetries, int retryDelaySeconds, double retryBackoff,
                         int timeoutSeconds, boolean singleton) {
        this(id, dagId, name, taskType, config, maxRetries, retryDelaySeconds, retryBackoff,
                timeoutSeconds, singleton, 0);
    }

    public DagTaskEntity(UUID id, UUID dagId, String name, String taskType, String config,
                         int maxRetries, int retryDelaySeconds, double retryBackoff,
                         int timeoutSeconds, boolean singleton, int version) {
        this.id = id;
        this.dagId = dagId;
        this.name = name;
        this.taskType = taskType;
        this.config = config;
        this.maxRetries = maxRetries;
        this.retryDelaySeconds = retryDelaySeconds;
        this.retryBackoff = retryBackoff;
        this.timeoutSeconds = timeoutSeconds;
        this.singleton = singleton;
        this.version = version;
    }

    public UUID getId() {
        return id;
    }

    public UUID getDagId() {
        return dagId;
    }

    public String getName() {
        return name;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getConfig() {
        return config;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getRetryDelaySeconds() {
        return retryDelaySeconds;
    }

    public double getRetryBackoff() {
        return retryBackoff;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public int getVersion() {
        return version;
    }

    public boolean isSingleton() {
        return singleton;
    }
}
