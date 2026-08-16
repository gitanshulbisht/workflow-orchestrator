package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.persistence.DeadLetterEntity;
import com.buildathon.orchestrator.persistence.WebhookEntity;
import com.buildathon.orchestrator.service.DeadLetterService;
import com.buildathon.orchestrator.service.WebhookService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Dead Letters & Webhooks", description = "DLQ management and webhook subscriptions")
public class DeadLetterAndWebhookController {

    private final DeadLetterService deadLetterService;
    private final WebhookService webhookService;
    private final ObjectMapper objectMapper;

    public DeadLetterAndWebhookController(DeadLetterService deadLetterService,
                                          WebhookService webhookService,
                                          ObjectMapper objectMapper) {
        this.deadLetterService = deadLetterService;
        this.webhookService = webhookService;
        this.objectMapper = objectMapper;
    }

    @Operation(summary = "List dead-lettered tasks")
    @GetMapping("/dead-letters")
    public List<Map<String, Object>> listDeadLetters(@RequestParam(defaultValue = "50") int limit) {
        return deadLetterService.list(limit).stream().map(this::toDlMap).toList();
    }

    @Operation(summary = "Replay a dead-lettered task")
    @PostMapping("/dead-letters/{id}/replay")
    public Map<String, Object> replay(@PathVariable UUID id) {
        return toDlMap(deadLetterService.replay(id));
    }

    @Operation(summary = "List webhooks")
    @GetMapping("/webhooks")
    public List<Map<String, Object>> listWebhooks() {
        return webhookService.list().stream().map(this::toWebhookMap).toList();
    }

    @Operation(summary = "Register a webhook subscription")
    @PostMapping("/webhooks")
    public ResponseEntity<Map<String, Object>> registerWebhook(@RequestBody Map<String, Object> body) {
        String url = (String) body.get("url");
        String secret = (String) body.getOrDefault("secret", "");
        List<String> events = body.get("events") instanceof List<?> list
                ? list.stream().map(String::valueOf).toList()
                : List.of();
        WebhookEntity webhook = webhookService.register(url, secret, events);
        return ResponseEntity.created(URI.create("/api/v1/webhooks/" + webhook.getId()))
                .body(toWebhookMap(webhook));
    }

    @Operation(summary = "Enable/disable a webhook")
    @PatchMapping("/webhooks/{id}")
    public Map<String, Object> toggleWebhook(@PathVariable UUID id, @RequestBody Map<String, Object> body) {
        boolean active = (Boolean) body.getOrDefault("active", true);
        return toWebhookMap(webhookService.toggle(id, active));
    }

    private Map<String, Object> toDlMap(DeadLetterEntity dl) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", dl.getId());
        map.put("taskInstanceId", dl.getTaskInstanceId());
        map.put("runId", dl.getRunId());
        map.put("taskName", dl.getTaskName());
        map.put("errorPayload", parseJson(dl.getErrorPayload()));
        map.put("deadLetteredAt", dl.getDeadLetteredAt());
        map.put("replayStatus", dl.getReplayStatus());
        return map;
    }

    private Map<String, Object> toWebhookMap(WebhookEntity webhook) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", webhook.getId());
        map.put("url", webhook.getUrl());
        map.put("events", parseJson(webhook.getSubscribedEvents()));
        map.put("active", webhook.isActive());
        map.put("createdAt", webhook.getCreatedAt());
        return map;
    }

    private Object parseJson(String json) {
        try {
            return objectMapper.readValue(json, Object.class);
        } catch (Exception e) {
            return Map.of();
        }
    }
}
