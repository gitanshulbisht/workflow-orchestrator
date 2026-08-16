package com.buildathon.orchestrator.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RunStateMachineTest {

    @ParameterizedTest
    @EnumSource(RunState.class)
    void allowsEveryDocumentedTransition(RunState from) {
        for (RunState to : RunState.values()) {
            boolean allowed = switch (from) {
                case PENDING -> to == RunState.RUNNING || to == RunState.CANCELLED;
                case RUNNING -> to == RunState.SUCCESS || to == RunState.FAILED || to == RunState.CANCELLED;
                default -> false;
            };
            assertThat(RunStateMachine.canTransition(from, to))
                    .as("%s -> %s", from, to)
                    .isEqualTo(allowed);
        }
    }

    @Test
    void rejectsIllegalTransitions() {
        assertThatThrownBy(() -> RunStateMachine.requireTransition(RunState.FAILED, RunState.RUNNING))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RunStateMachine.requireTransition(RunState.SUCCESS, RunState.PENDING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void terminalStatesAreRecognized() {
        assertThat(RunStateMachine.isTerminal(RunState.SUCCESS)).isTrue();
        assertThat(RunStateMachine.isTerminal(RunState.FAILED)).isTrue();
        assertThat(RunStateMachine.isTerminal(RunState.CANCELLED)).isTrue();
        assertThat(RunStateMachine.isTerminal(RunState.PENDING)).isFalse();
        assertThat(RunStateMachine.isTerminal(RunState.RUNNING)).isFalse();
    }
}
