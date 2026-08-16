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
@Table(name = "webhook")
public class WebhookEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String url;

    @Column(nullable = false)
    private String secret;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "subscribed_events", nullable = false, columnDefinition = "jsonb")
    private String subscribedEvents;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected WebhookEntity() {
    }

    public WebhookEntity(UUID id, String url, String secret, String subscribedEvents, Instant createdAt) {
        this.id = id;
        this.url = url;
        this.secret = secret;
        this.subscribedEvents = subscribedEvents;
        this.active = true;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public String getSecret() {
        return secret;
    }

    public String getSubscribedEvents() {
        return subscribedEvents;
    }

    public boolean isActive() {
        return active;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
