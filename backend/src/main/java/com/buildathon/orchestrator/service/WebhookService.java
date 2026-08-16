package com.buildathon.orchestrator.service;

import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.WebhookEntity;
import com.buildathon.orchestrator.persistence.WebhookRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class WebhookService {

    private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

    private final WebhookRepository webhookRepository;
    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;

    public WebhookService(WebhookRepository webhookRepository, OutboxWriter outboxWriter, ObjectMapper objectMapper) {
        this.webhookRepository = webhookRepository;
        this.outboxWriter = outboxWriter;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public WebhookEntity register(String url, String secret, List<String> events) {
        WebhookEntity webhook = new WebhookEntity(UUID.randomUUID(), url, secret, toJson(events), Instant.now());
        webhookRepository.save(webhook);
        outboxWriter.write("WEBHOOK", webhook.getId().toString(), "WEBHOOK_REGISTERED",
                Map.of("webhookId", webhook.getId().toString(), "url", url));
        log.info("Registered webhook {} -> {}", webhook.getId(), url);
        return webhook;
    }

    @Transactional(readOnly = true)
    public List<WebhookEntity> list() {
        return webhookRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<WebhookEntity> active() {
        return webhookRepository.findByActiveTrue();
    }

    @Transactional
    public WebhookEntity toggle(UUID id, boolean active) {
        WebhookEntity webhook = webhookRepository.findById(id)
                .orElseThrow(() -> new DagService.NotFoundException("Webhook not found: " + id));
        webhook.setActive(active);
        return webhookRepository.save(webhook);
    }

    private String toJson(List<String> events) {
        try {
            return objectMapper.writeValueAsString(events == null ? List.of() : events);
        } catch (Exception e) {
            return "[]";
        }
    }
}
