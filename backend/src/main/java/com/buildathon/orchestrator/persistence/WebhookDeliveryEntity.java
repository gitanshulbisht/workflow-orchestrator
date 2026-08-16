package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "webhook_delivery")
public class WebhookDeliveryEntity {

    @Id
    private UUID id;

    @Column(name = "webhook_id", nullable = false)
    private UUID webhookId;

    @Column(name = "event_id", nullable = false)
    private Long eventId;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "delivered_at")
    private Instant deliveredAt;

    protected WebhookDeliveryEntity() {
    }

    public WebhookDeliveryEntity(UUID id, UUID webhookId, Long eventId) {
        this.id = id;
        this.webhookId = webhookId;
        this.eventId = eventId;
        this.status = "PENDING";
        this.attempts = 0;
    }

    public UUID getId() {
        return id;
    }

    public UUID getWebhookId() {
        return webhookId;
    }

    public Long getEventId() {
        return eventId;
    }

    public String getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getDeliveredAt() {
        return deliveredAt;
    }

    public void recordAttempt(String error) {
        this.attempts++;
        this.lastError = error;
    }

    public void markDelivered(Instant now) {
        this.status = "DELIVERED";
        this.deliveredAt = now;
    }

    public void markFailed() {
        this.status = "FAILED";
    }
}
