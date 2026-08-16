package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Subscribes to the outbox event channel and broadcasts to SSE subscribers.
 * Runs only on instances with the "api" role.
 */
@Component
public class EventFanOut implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(EventFanOut.class);

    private final RedisMessageListenerContainer container;
    private final SseBroadcaster broadcaster;
    private final OrchestratorProperties properties;

    public EventFanOut(RedisMessageListenerContainer container, SseBroadcaster broadcaster,
                       OrchestratorProperties properties) {
        this.container = container;
        this.broadcaster = broadcaster;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!properties.hasRole("api")) {
            return;
        }
        container.addMessageListener(this, new ChannelTopic(RedisEventPublisher.CHANNEL));
        log.info("Subscribed to Redis channel {} for SSE fan-out", RedisEventPublisher.CHANNEL);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String json = new String(message.getBody());
        try {
            var node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
            String eventType = node.get("eventType").asText();
            String payload = node.get("payload").toString();
            broadcaster.broadcast(eventType, payload);
        } catch (Exception e) {
            log.warn("Could not parse event message: {}", json, e);
        }
    }
}
