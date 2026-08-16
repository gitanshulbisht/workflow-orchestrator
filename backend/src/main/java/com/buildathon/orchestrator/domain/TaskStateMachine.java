package com.buildathon.orchestrator.domain;

import java.util.Map;
import java.util.Set;

/**
 * Pure task state machine. Validates every transition; illegal transitions are rejected.
 */
public final class TaskStateMachine {

    private static final Map<TaskState, Set<TaskState>> ALLOWED = Map.ofEntries(
            Map.entry(TaskState.PENDING, Set.of(TaskState.SCHEDULED, TaskState.SKIPPED, TaskState.CANCELLED)),
            Map.entry(TaskState.SCHEDULED, Set.of(TaskState.RUNNING, TaskState.CANCELLED)),
            Map.entry(TaskState.RUNNING, Set.of(TaskState.SUCCESS, TaskState.FAILED, TaskState.CANCELLED)),
            Map.entry(TaskState.FAILED, Set.of(TaskState.UP_FOR_RETRY, TaskState.DEAD_LETTERED)),
            Map.entry(TaskState.UP_FOR_RETRY, Set.of(TaskState.SCHEDULED)),
            Map.entry(TaskState.DEAD_LETTERED, Set.of(TaskState.SCHEDULED)),
            Map.entry(TaskState.SKIPPED, Set.of()),
            Map.entry(TaskState.CANCELLED, Set.of()),
            Map.entry(TaskState.SUCCESS, Set.of())
    );

    private TaskStateMachine() {
    }

    public static void requireTransition(TaskState from, TaskState to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateException("Illegal task state transition: " + from + " -> " + to);
        }
    }

    public static boolean canTransition(TaskState from, TaskState to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    public static boolean isTerminal(TaskState state) {
        return switch (state) {
            case SUCCESS, DEAD_LETTERED, SKIPPED, CANCELLED -> true;
            default -> false;
        };
    }
}
