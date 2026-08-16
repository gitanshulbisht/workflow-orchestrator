package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.outbox.SseBroadcaster;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@Tag(name = "Events", description = "Live event stream")
public class EventsController {

    private final SseBroadcaster broadcaster;

    public EventsController(SseBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Operation(summary = "Subscribe to live outbox events via SSE")
    @GetMapping(value = "/api/v1/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }
}
