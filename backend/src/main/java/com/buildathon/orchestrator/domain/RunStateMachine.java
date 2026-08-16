package com.buildathon.orchestrator.domain;

import java.util.Map;
import java.util.Set;

/**
 * Pure run state machine. A run is terminal when SUCCESS/FAILED/CANCELLED.
 */
public final class RunStateMachine {

    private static final Map<RunState, Set<RunState>> ALLOWED = Map.of(
            RunState.PENDING, Set.of(RunState.RUNNING, RunState.CANCELLED),
            RunState.RUNNING, Set.of(RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED),
            RunState.SUCCESS, Set.of(),
            RunState.FAILED, Set.of(),
            RunState.CANCELLED, Set.of()
    );

    private RunStateMachine() {
    }

    public static void requireTransition(RunState from, RunState to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateException("Illegal run state transition: " + from + " -> " + to);
        }
    }

    public static boolean canTransition(RunState from, RunState to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    public static boolean isTerminal(RunState state) {
        return switch (state) {
            case SUCCESS, FAILED, CANCELLED -> true;
            default -> false;
        };
    }
}
