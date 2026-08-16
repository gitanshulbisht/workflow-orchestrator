package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes shell commands in the worker's container. Timeout enforced via
 * process destroy; output tail stored on the attempt.
 */
public class BashExecutor implements TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(BashExecutor.class);

    @Override
    public String type() {
        return "bash";
    }

    @Override
    public TaskExecutionResult execute(TaskInstanceEntity instance, Map<String, Object> config, int timeoutSeconds) throws Exception {
        Object rawCommand = config.get("command");
        if (!(rawCommand instanceof String command) || command.isBlank()) {
            return TaskExecutionResult.failure("bash task requires a 'command' config", null);
        }
        int effectiveTimeout = timeoutSeconds > 0 ? timeoutSeconds : 300;
        Process process = new ProcessBuilder("/bin/sh", "-c", command)
                .redirectErrorStream(true)
                .start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Thread drainer = Thread.ofVirtual().start(() -> {
            try (var in = process.getInputStream()) {
                in.transferTo(output);
            } catch (Exception ignored) {
            }
        });
        boolean finished = process.waitFor(effectiveTimeout, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            return TaskExecutionResult.failure("Command timed out after " + effectiveTimeout + "s", tail(output));
        }
        drainer.join(2000);
        int exitCode = process.exitValue();
        String logTail = tail(output);
        if (exitCode != 0) {
            return TaskExecutionResult.failure("Command exited with code " + exitCode, logTail);
        }
        return TaskExecutionResult.success(logTail);
    }

    private String tail(ByteArrayOutputStream output) {
        String all = output.toString();
        if (all.length() <= 4000) {
            return all;
        }
        return "..." + all.substring(all.length() - 4000);
    }
}
