package com.buildathon.orchestrator.api.dto;

import java.util.List;
import java.util.UUID;

public record DagResponse(
        UUID id,
        String name,
        String description,
        int version,
        String scheduleCron,
        String timezone,
        boolean paused,
        String yaml,
        List<TaskResponse> tasks
) {
    public record TaskResponse(
            UUID id,
            String name,
            String type,
            java.util.Map<String, Object> config,
            int maxRetries,
            int retryDelaySeconds,
            double retryBackoff,
            int timeoutSeconds,
            boolean singleton,
            List<UUID> dependsOn
    ) {
    }
}
