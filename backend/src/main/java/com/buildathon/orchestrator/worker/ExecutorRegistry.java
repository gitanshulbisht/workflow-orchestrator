package com.buildathon.orchestrator.worker;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of task executors keyed by task type.
 */
@Component
public class ExecutorRegistry {

    private final Map<String, TaskExecutor> executors = new HashMap<>();

    public ExecutorRegistry(List<TaskExecutor> executorList) {
        executorList.forEach(executor -> executors.put(executor.type(), executor));
    }

    public TaskExecutor forType(String type) {
        TaskExecutor executor = executors.get(type);
        if (executor == null) {
            throw new IllegalArgumentException("No executor for task type: " + type);
        }
        return executor;
    }
}
