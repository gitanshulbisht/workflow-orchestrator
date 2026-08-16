package com.buildathon.orchestrator.domain;

import java.security.SecureRandom;

/**
 * Exponential backoff with full jitter: delay * backoff^attempt, randomized in [0, computed].
 */
public final class BackoffCalculator {

    private static final SecureRandom RANDOM = new SecureRandom();

    private BackoffCalculator() {
    }

    /**
     * @param attemptNo the upcoming attempt number (1-based)
     */
    public static long computeDelayMillis(int baseDelaySeconds, double backoff, int attemptNo) {
        if (attemptNo < 1) {
            throw new IllegalArgumentException("attemptNo must be >= 1");
        }
        double raw = baseDelaySeconds * Math.pow(backoff, attemptNo - 1) * 1000.0;
        return (long) (RANDOM.nextDouble() * raw);
    }

    /** Deterministic variant used by tests and for scheduling in a fixed window. */
    public static long maxDelayMillis(int baseDelaySeconds, double backoff, int attemptNo) {
        return (long) (baseDelaySeconds * Math.pow(backoff, attemptNo - 1) * 1000.0);
    }
}
