package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Pluggable task execution strategy. One executor per task type.
 */
public interface TaskExecutor {

    String type();

    TaskExecutionResult execute(TaskInstanceEntity instance, Map<String, Object> config, int timeoutSeconds) throws Exception;
}
