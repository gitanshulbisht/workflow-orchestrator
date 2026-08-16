package com.buildathon.orchestrator.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-process SSE broadcaster. Subscribers connect to /api/v1/events/stream and
 * receive outbox events as they are relayed.
 */
@Component
public class SseBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(SseBroadcaster.class);

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        return register(new SseEmitter(30 * 60_000L));
    }

    public SseEmitter register(SseEmitter emitter) {
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        try {
            emitter.send(SseEmitter.event().name("connected").data("{\"status\":\"connected\"}"));
        } catch (Exception e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    public void broadcast(String eventType, String payload) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name(eventType)
                        .data(payload == null ? "{}" : payload));
            } catch (Exception e) {
                emitters.remove(emitter);
            }
        }
    }
}
