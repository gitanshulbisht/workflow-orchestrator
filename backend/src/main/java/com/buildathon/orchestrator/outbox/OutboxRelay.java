package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.persistence.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Publishes pending outbox events and marks them PUBLISHED. Poll-based and
 * crash-safe: a crash between publish and mark simply re-publishes, which is
 * acceptable for at-least-once fan-out.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository repository;
    private final EventPublisher publisher;

    public OutboxRelay(OutboxEventRepository repository, EventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    public int publishPending() {
        var pending = repository.findByDeliveryStatusOrderByIdAsc("PENDING", PageRequest.of(0, 100));
        int published = 0;
        for (var event : pending) {
            publisher.publish(event);
            event.markPublished(Instant.now());
            repository.save(event);
            published++;
        }
        if (published > 0) {
            log.debug("Published {} outbox events", published);
        }
        return published;
    }
}
