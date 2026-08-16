package com.buildathon.orchestrator.domain;

public enum TaskState {
    PENDING,
    SCHEDULED,
    RUNNING,
    SUCCESS,
    FAILED,
    UP_FOR_RETRY,
    DEAD_LETTERED,
    SKIPPED,
    CANCELLED
}
