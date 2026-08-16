package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;

import java.util.Map;

/**
 * Sleeps for the configured number of seconds — used in demos and tests.
 */
public class DelayExecutor implements TaskExecutor {

    @Override
    public String type() {
        return "delay";
    }

    @Override
    public TaskExecutionResult execute(TaskInstanceEntity instance, Map<String, Object> config, int timeoutSeconds) throws Exception {
        Object rawSeconds = config.get("seconds");
        int seconds = rawSeconds instanceof Number n ? Math.min(n.intValue(), 3600) : 0;
        if (seconds > 0) {
            Thread.sleep(seconds * 1000L);
        }
        return TaskExecutionResult.success("slept " + seconds + "s");
    }
}
