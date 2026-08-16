package com.buildathon.orchestrator.domain;

import java.util.List;

public record TaskSpec(
        String name,
        String type,
        java.util.Map<String, Object> config,
        int maxRetries,
        int retryDelaySeconds,
        double retryBackoff,
        int timeoutSeconds,
        boolean singleton,
        List<String> dependsOn
) {
    public TaskSpec {
        config = config == null ? java.util.Map.of() : config;
        dependsOn = dependsOn == null ? List.of() : dependsOn;
        if (maxRetries < 0) {
            maxRetries = 0;
        }
        if (retryDelaySeconds <= 0) {
            retryDelaySeconds = 5;
        }
        if (retryBackoff <= 0) {
            retryBackoff = 2.0;
        }
        if (timeoutSeconds <= 0) {
            timeoutSeconds = 300;
        }
    }
}
