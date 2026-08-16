package com.buildathon.orchestrator.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DagValidatorTest {

    private static TaskSpec task(String name, String... deps) {
        return new TaskSpec(name, "delay", java.util.Map.of("seconds", 1), 0, 5, 2.0, 60, false, List.of(deps));
    }

    @Test
    void acceptsValidDag() {
        DagSpec spec = new DagSpec("ok", null, null, "UTC",
                List.of(task("a"), task("b", "a"), task("c", "a", "b")));
        assertThat(DagValidator.validate(spec)).isEmpty();
    }

    @Test
    void acceptsParallelBranches() {
        DagSpec spec = new DagSpec("ok", null, null, "UTC",
                List.of(task("start"), task("left", "start"), task("right", "start"), task("join", "left", "right")));
        assertThat(DagValidator.validate(spec)).isEmpty();
    }

    @Test
    void rejectsDirectCycle() {
        DagSpec spec = new DagSpec("cycle", null, null, "UTC",
                List.of(task("a", "b"), task("b", "a")));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).startsWith("cycle detected"));
    }

    @Test
    void rejectsIndirectCycle() {
        DagSpec spec = new DagSpec("cycle3", null, null, "UTC",
                List.of(task("a", "c"), task("b", "a"), task("c", "b")));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).startsWith("cycle detected"));
    }

    @Test
    void rejectsUnknownDependency() {
        DagSpec spec = new DagSpec("unknown-dep", null, null, "UTC",
                List.of(task("a", "ghost")));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("unknown task 'ghost'"));
    }

    @Test
    void rejectsDuplicateTaskNames() {
        DagSpec spec = new DagSpec("dup", null, null, "UTC",
                List.of(task("a"), task("a")));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("duplicate task name: a"));
    }

    @Test
    void rejectsMissingName() {
        DagSpec spec = new DagSpec("  ", null, null, "UTC", List.of(task("a")));
        assertThat(DagValidator.validate(spec)).contains("name is required");
    }

    @Test
    void rejectsEmptyTaskList() {
        DagSpec spec = new DagSpec("empty", null, null, "UTC", List.of());
        assertThat(DagValidator.validate(spec)).contains("at least one task is required");
    }

    @Test
    void rejectsUnknownTaskType() {
        TaskSpec bad = new TaskSpec("t", "quantum", java.util.Map.of(), 0, 5, 2.0, 60, false, List.of());
        DagSpec spec = new DagSpec("bad-type", null, null, "UTC", List.of(bad));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("type must be one of"));
    }

    @Test
    void rejectsBashWithoutCommand() {
        TaskSpec bad = new TaskSpec("t", "bash", java.util.Map.of(), 0, 5, 2.0, 60, false, List.of());
        DagSpec spec = new DagSpec("bad-bash", null, null, "UTC", List.of(bad));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("require a 'command'"));
    }

    @Test
    void rejectsRetryPolicyOutOfBounds() {
        TaskSpec bad = new TaskSpec("t", "delay", java.util.Map.of(), DagValidator.MAX_RETRIES + 1, 5, 2.0, 60, false, List.of());
        DagSpec spec = new DagSpec("bad-retry", null, null, "UTC", List.of(bad));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("maxRetries"));
    }

    @Test
    void rejectsInvalidCron() {
        DagSpec spec = new DagSpec("bad-cron", null, "not-a-cron * *", "UTC", List.of(task("a")));
        assertThat(DagValidator.validate(spec))
                .anySatisfy(err -> assertThat(err).contains("invalid cron"));
    }

    @Test
    void acceptsValidCron() {
        DagSpec spec = new DagSpec("good-cron", null, "0 0 * * * ?", "UTC", List.of(task("a")));
        assertThat(DagValidator.validate(spec)).isEmpty();
    }
}
