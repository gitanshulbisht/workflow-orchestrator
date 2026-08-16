package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.persistence.OutboxEventEntity;

@FunctionalInterface
public interface EventPublisher {

    void publish(OutboxEventEntity event);
}
