package com.buildathon.orchestrator.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DagParserTest {

    @Test
    void parsesValidYamlDefinition() {
        String yaml = """
                name: data-pipeline
                description: Fetch and process data
                tasks:
                  - name: fetch
                    type: bash
                    config:
                      command: echo hello
                    maxRetries: 2
                    retryDelaySeconds: 5
                    retryBackoff: 2.0
                    timeoutSeconds: 60
                  - name: process
                    type: bash
                    config:
                      command: echo world
                    dependsOn: [fetch]
                """;

        DagSpec spec = DagParser.parse(yaml);

        assertThat(spec.name()).isEqualTo("data-pipeline");
        assertThat(spec.tasks()).hasSize(2);
        assertThat(spec.tasks().get(0).name()).isEqualTo("fetch");
        assertThat(spec.tasks().get(0).config().get("command")).isEqualTo("echo hello");
        assertThat(spec.tasks().get(1).dependsOn()).containsExactly("fetch");
    }

    @Test
    void parsesJsonDefinition() {
        String json = """
                {
                  "name": "json-dag",
                  "tasks": [
                    {"name": "a", "type": "delay", "config": {"seconds": 1}}
                  ]
                }
                """;

        DagSpec spec = DagParser.parse(json);

        assertThat(spec.name()).isEqualTo("json-dag");
        assertThat(spec.tasks()).hasSize(1);
    }

    @Test
    void rejectsEmptyAndGarbage() {
        assertThatThrownBy(() -> DagParser.parse("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DagParser.parse("{{{{ not yaml")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void roundTripsThroughJson() {
        DagSpec spec = new DagSpec("round-trip", null, null, "UTC",
                java.util.List.of(new TaskSpec("t", "fail", java.util.Map.of(), 1, 5, 2.0, 60, false, java.util.List.of())));

        DagSpec back = DagParser.fromJson(DagParser.toJson(spec));

        assertThat(back).isEqualTo(spec);
    }
}
