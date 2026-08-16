package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Outbound HTTP task. Sends an idempotency key derived from run/task/attempt
 * so retried attempts are safe for the receiver.
 */
public class HttpExecutor implements TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(HttpExecutor.class);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Override
    public String type() {
        return "http";
    }

    @Override
    public TaskExecutionResult execute(TaskInstanceEntity instance, Map<String, Object> config, int timeoutSeconds) throws Exception {
        String url = (String) config.get("url");
        String method = config.get("method") instanceof String m ? m.toUpperCase() : "POST";
        Map<?, ?> headers = config.get("headers") instanceof Map<?, ?> h ? h : Map.of();
        Object rawBody = config.get("body");

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(Math.max(timeoutSeconds, 5)));
        headers.forEach((k, v) -> builder.header(String.valueOf(k), String.valueOf(v)));

        // Idempotency at the attempt level.
        builder.header("Idempotency-Key", instance.getRunId() + ":" + instance.getId() + ":" + (instance.getAttemptNo() + 1));

        if (rawBody == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json");
            builder.method(method, HttpRequest.BodyPublishers.ofString(String.valueOf(rawBody)));
        }

        try {
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            String body = response.body();
            String tail = body != null && body.length() > 4000 ? body.substring(body.length() - 4000) : body;
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return TaskExecutionResult.success("HTTP " + response.statusCode() + "\n" + tail);
            }
            return TaskExecutionResult.failure("HTTP " + response.statusCode(), tail);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return TaskExecutionResult.failure("HTTP request failed: " + e.getMessage(), null);
        }
    }
}
