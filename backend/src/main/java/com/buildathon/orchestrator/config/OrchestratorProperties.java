package com.buildathon.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "orchestrator")
public record OrchestratorProperties(
        List<String> roles,
        Worker worker,
        Scheduler scheduler,
        Outbox outbox,
        RateLimit rateLimit,
        Webhook webhook,
        Lock lock,
        Idempotency idempotency
) {

    public record Worker(
            int concurrency,
            long pollIntervalMs,
            long heartbeatIntervalMs,
            long staleHeartbeatMs
    ) {
    }

    public record Scheduler(long scanIntervalMs) {
    }

    public record Outbox(long relayIntervalMs, int batchSize) {
    }

    public record RateLimit(boolean enabled, int permitsPerSecond, int burst) {
    }

    public record Webhook(int maxDeliveryAttempts, long deliveryTimeoutMs) {
    }

    public record Lock(long leaseTimeMs) {
    }

    public record Idempotency(long ttlMinutes) {
    }

    public boolean hasRole(String role) {
        return roles != null && roles.contains(role);
    }
}
