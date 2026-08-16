package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

@Entity
@Table(name = "idempotency_record")
public class IdempotencyRecordEntity implements Persistable<String> {

    @Id
    @Column(name = "key")
    private String key;

    @Column(name = "request_hash", nullable = false)
    private String requestHash;

    @Column(name = "status_code", nullable = false)
    private int statusCode;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body", columnDefinition = "jsonb")
    private String responseBody;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Transient
    private boolean isNew = true;

    protected IdempotencyRecordEntity() {
    }

    public IdempotencyRecordEntity(String key, String requestHash, int statusCode, String responseBody, Instant expiresAt) {
        this.key = key;
        this.requestHash = requestHash;
        this.statusCode = statusCode;
        this.responseBody = responseBody;
        this.expiresAt = expiresAt;
    }

    public String getKey() {
        return key;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    @Override
    public String getId() {
        return key;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @jakarta.persistence.PostLoad
    void postLoad() {
        this.isNew = false;
    }

    public void complete(String requestHash, int statusCode, String responseBody) {
        this.requestHash = requestHash;
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}
