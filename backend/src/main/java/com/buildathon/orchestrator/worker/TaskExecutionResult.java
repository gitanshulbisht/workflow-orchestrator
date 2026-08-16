package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;

import java.util.Map;

public record TaskExecutionResult(int exitCode, String logTail, String error) {

    public boolean isSuccess() {
        return exitCode == 0 && error == null;
    }

    public static TaskExecutionResult success(String logTail) {
        return new TaskExecutionResult(0, logTail, null);
    }

    public static TaskExecutionResult failure(String error, String logTail) {
        return new TaskExecutionResult(1, logTail, error);
    }
}
