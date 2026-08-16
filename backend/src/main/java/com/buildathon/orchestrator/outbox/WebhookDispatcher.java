package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import com.buildathon.orchestrator.persistence.OutboxEventEntity;
import com.buildathon.orchestrator.persistence.OutboxEventRepository;
import com.buildathon.orchestrator.persistence.WebhookDeliveryEntity;
import com.buildathon.orchestrator.persistence.WebhookDeliveryRepository;
import com.buildathon.orchestrator.persistence.WebhookEntity;
import com.buildathon.orchestrator.service.WebhookService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Delivers published outbox events to subscribed webhooks with HMAC-signed
 * payloads, per-webhook retries, and delivery status tracking.
 */
@Component
public class WebhookDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

    private final WebhookService webhookService;
    private final OutboxEventRepository outboxEventRepository;
    private final WebhookDeliveryRepository deliveryRepository;
    private final OrchestratorProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public WebhookDispatcher(WebhookService webhookService, OutboxEventRepository outboxEventRepository,
                             WebhookDeliveryRepository deliveryRepository, OrchestratorProperties properties,
                             ObjectMapper objectMapper) {
        this.webhookService = webhookService;
        this.outboxEventRepository = outboxEventRepository;
        this.deliveryRepository = deliveryRepository;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * For every published event not yet dispatched, create delivery records for
     * subscribed webhooks and attempt delivery with retries.
     */
    public int dispatchPending() {
        var events = outboxEventRepository.findByDeliveryStatusOrderByIdAsc(
                OutboxEventEntity.STATUS_PUBLISHED, PageRequest.of(0, 50));
        int delivered = 0;
        for (OutboxEventEntity event : events) {
            for (WebhookEntity webhook : webhookService.active()) {
                if (!subscribes(webhook, event.getEventType())) {
                    continue;
                }
                delivered += deliverToWebhook(webhook, event);
            }
        }
        return delivered;
    }

    private int deliverToWebhook(WebhookEntity webhook, OutboxEventEntity event) {
        WebhookDeliveryEntity delivery = new WebhookDeliveryEntity(UUID.randomUUID(), webhook.getId(), event.getId());
        deliveryRepository.save(delivery);

        String payload = buildPayload(event);
        String signature = sign(webhook.getSecret(), payload);

        int maxAttempts = properties.webhook().maxDeliveryAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(webhook.getUrl()))
                        .timeout(Duration.ofMillis(properties.webhook().deliveryTimeoutMs()))
                        .header("Content-Type", "application/json")
                        .header("X-Orchestrator-Signature", "sha256=" + signature)
                        .header("X-Orchestrator-Event", event.getEventType())
                        .header("X-Orchestrator-Delivery", delivery.getId().toString())
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    delivery.markDelivered(Instant.now());
                    deliveryRepository.save(delivery);
                    log.debug("Delivered event {} to webhook {}", event.getEventType(), webhook.getId());
                    return 1;
                }
                delivery.recordAttempt("HTTP " + response.statusCode());
                deliveryRepository.save(delivery);
            } catch (Exception e) {
                delivery.recordAttempt(e.getMessage());
                deliveryRepository.save(delivery);
            }
            try {
                Thread.sleep(500L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        delivery.markFailed();
        deliveryRepository.save(delivery);
        log.warn("Webhook {} failed to receive event {} after {} attempts",
                webhook.getId(), event.getEventType(), maxAttempts);
        return 0;
    }

    private boolean subscribes(WebhookEntity webhook, String eventType) {
        try {
            List<String> events = objectMapper.readValue(webhook.getSubscribedEvents(),
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {
                    });
            return events.isEmpty() || events.contains(eventType);
        } catch (Exception e) {
            return false;
        }
    }

    private String buildPayload(OutboxEventEntity event) {
        try {
            Map<String, Object> payload = Map.of(
                    "id", event.getId(),
                    "aggregateType", event.getAggregateType(),
                    "aggregateId", event.getAggregateId(),
                    "eventType", event.getEventType(),
                    "payload", objectMapper.readTree(event.getPayload()),
                    "createdAt", event.getCreatedAt().toString());
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return "{}";
        }
    }

    private String sign(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(), "HmacSHA256"));
            return Base64.getEncoder().encodeToString(mac.doFinal(payload.getBytes()));
        } catch (Exception e) {
            return "";
        }
    }
}
