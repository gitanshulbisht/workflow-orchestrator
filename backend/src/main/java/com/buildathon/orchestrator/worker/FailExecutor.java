package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;

import java.util.Map;

/**
 * Deterministic failure — used in demos and tests to exercise retry/DLQ.
 */
public class FailExecutor implements TaskExecutor {

    @Override
    public String type() {
        return "fail";
    }

    @Override
    public TaskExecutionResult execute(TaskInstanceEntity instance, Map<String, Object> config, int timeoutSeconds) throws Exception {
        String message = config.get("message") instanceof String m ? m : "fail task executed";
        return TaskExecutionResult.failure(message, null);
    }
}
