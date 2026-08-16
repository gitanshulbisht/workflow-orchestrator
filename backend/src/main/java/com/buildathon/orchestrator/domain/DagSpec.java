package com.buildathon.orchestrator.domain;

public record DagSpec(
        String name,
        String description,
        String scheduleCron,
        String timezone,
        java.util.List<TaskSpec> tasks
) {}
