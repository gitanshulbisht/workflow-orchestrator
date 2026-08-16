package com.buildathon.orchestrator.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskStateMachineTest {

    @ParameterizedTest
    @EnumSource(TaskState.class)
    void allowsEveryDocumentedTransition(TaskState from) {
        for (TaskState to : TaskState.values()) {
            boolean allowed = switch (from) {
                case PENDING -> to == TaskState.SCHEDULED || to == TaskState.SKIPPED || to == TaskState.CANCELLED;
                case SCHEDULED -> to == TaskState.RUNNING || to == TaskState.CANCELLED;
                case RUNNING -> to == TaskState.SUCCESS || to == TaskState.FAILED || to == TaskState.CANCELLED;
                case FAILED -> to == TaskState.UP_FOR_RETRY || to == TaskState.DEAD_LETTERED;
                case UP_FOR_RETRY -> to == TaskState.SCHEDULED;
                case DEAD_LETTERED -> to == TaskState.SCHEDULED;
                default -> false;
            };
            assertThat(TaskStateMachine.canTransition(from, to))
                    .as("%s -> %s", from, to)
                    .isEqualTo(allowed);
        }
    }

    @Test
    void rejectsIllegalTransitions() {
        assertThatThrownBy(() -> TaskStateMachine.requireTransition(TaskState.SUCCESS, TaskState.RUNNING))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SUCCESS -> RUNNING");
        assertThatThrownBy(() -> TaskStateMachine.requireTransition(TaskState.PENDING, TaskState.RUNNING))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> TaskStateMachine.requireTransition(TaskState.SKIPPED, TaskState.RUNNING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void terminalStatesAreRecognized() {
        assertThat(TaskStateMachine.isTerminal(TaskState.SUCCESS)).isTrue();
        assertThat(TaskStateMachine.isTerminal(TaskState.DEAD_LETTERED)).isTrue();
        assertThat(TaskStateMachine.isTerminal(TaskState.SKIPPED)).isTrue();
        assertThat(TaskStateMachine.isTerminal(TaskState.CANCELLED)).isTrue();
        assertThat(TaskStateMachine.isTerminal(TaskState.RUNNING)).isFalse();
        assertThat(TaskStateMachine.isTerminal(TaskState.UP_FOR_RETRY)).isFalse();
        assertThat(TaskStateMachine.isTerminal(TaskState.PENDING)).isFalse();
    }
}
