package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.persistence.OutboxEventEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes outbox events to Redis pub/sub for SSE and webhook fan-out.
 */
@Component
public class RedisEventPublisher implements EventPublisher {

    public static final String CHANNEL = "orchestrator:events";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisEventPublisher(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(OutboxEventEntity event) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("id", event.getId());
        envelope.put("aggregateType", event.getAggregateType());
        envelope.put("aggregateId", event.getAggregateId());
        envelope.put("eventType", event.getEventType());
        envelope.put("createdAt", event.getCreatedAt().toString());
        try {
            envelope.put("payload", objectMapper.readTree(event.getPayload()));
        } catch (Exception e) {
            envelope.put("payload", Map.of());
        }
        try {
            redisTemplate.convertAndSend(CHANNEL, objectMapper.writeValueAsString(envelope));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize outbox event", e);
        }
    }
}
