package com.buildathon.orchestrator.api.dto;

import java.time.Instant;

/**
 * RFC 7807 problem detail.
 */
public record ProblemDetail(
        String type,
        String title,
        int status,
        String detail,
        String instance,
        Instant timestamp
) {
    public static ProblemDetail of(int status, String title, String detail, String instance) {
        return new ProblemDetail("about:blank", title, status, detail, instance, Instant.now());
    }
}
