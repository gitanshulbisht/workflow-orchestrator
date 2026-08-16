package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the outbox relay on a fixed interval. Only active when the instance has
 * the "api" role.
 */
@Component
public class OutboxRelayRunner {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayRunner.class);

    private final OutboxRelay relay;
    private final OrchestratorProperties properties;

    public OutboxRelayRunner(OutboxRelay relay, OrchestratorProperties properties) {
        this.relay = relay;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!properties.hasRole("api")) {
            log.info("Outbox relay disabled (roles: {})", properties.roles());
        }
    }

    @Scheduled(fixedDelayString = "${orchestrator.outbox.relay-interval-ms:1000}")
    public void run() {
        if (!properties.hasRole("api")) {
            return;
        }
        relay.publishPending();
    }
}
