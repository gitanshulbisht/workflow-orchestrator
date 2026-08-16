package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.persistence.OutboxEventEntity;
import com.buildathon.orchestrator.persistence.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes outbox events inside the calling transaction. The relay publishes
 * them later, decoupling state changes from external fan-out.
 */
@Component
public class OutboxWriter {

    private static final Logger log = LoggerFactory.getLogger(OutboxWriter.class);

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxEventRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public void write(String aggregateType, String aggregateId, String eventType, Map<String, Object> payload) {
        String json;
        try {
            json = objectMapper.writeValueAsString(payload == null ? new LinkedHashMap<>() : payload);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize outbox payload", e);
        }
        repository.saveAndFlush(new OutboxEventEntity(aggregateType, aggregateId, eventType, json, Instant.now()));
        log.debug("Outbox event {} for {}:{}", eventType, aggregateType, aggregateId);
    }
}
