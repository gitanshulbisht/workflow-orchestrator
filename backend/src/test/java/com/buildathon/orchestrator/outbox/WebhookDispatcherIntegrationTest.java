package com.buildathon.orchestrator.outbox;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.persistence.OutboxEventEntity;
import com.buildathon.orchestrator.persistence.OutboxEventRepository;
import com.buildathon.orchestrator.persistence.WebhookEntity;
import com.buildathon.orchestrator.persistence.WebhookRepository;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WebhookDispatcherIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private WebhookDispatcher dispatcher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private WebhookRepository webhookRepository;

    private HttpServer server;
    private final List<String> receivedEvents = new CopyOnWriteArrayList<>();
    private final List<String> receivedSignatures = new CopyOnWriteArrayList<>();
    private final AtomicInteger requestCount = new AtomicInteger();

    @BeforeEach
    void startReceiver() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/hook", exchange -> {
            requestCount.incrementAndGet();
            receivedEvents.add(exchange.getRequestHeaders().getFirst("X-Orchestrator-Event"));
            receivedSignatures.add(exchange.getRequestHeaders().getFirst("X-Orchestrator-Signature"));
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });
        server.start();
    }

    @AfterEach
    void stopReceiver() {
        webhookRepository.deleteAll();
        outboxEventRepository.deleteAll();
        server.stop(0);
    }

    @Test
    void dispatchesPublishedEventsToSubscribedWebhooksWithSignature() {
        int port = server.getAddress().getPort();
        WebhookEntity webhook = webhookRepository.save(new WebhookEntity(
                UUID.randomUUID(), "http://127.0.0.1:" + port + "/hook", "secret",
                "[\"DAG_RUN_CREATED\"]", Instant.now()));

        OutboxEventEntity event = outboxEventRepository.save(new OutboxEventEntity(
                "DAG_RUN", UUID.randomUUID().toString(), "DAG_RUN_CREATED",
                "{\"hello\":\"world\"}", Instant.now()));
        event.markPublished(Instant.now());
        outboxEventRepository.save(event);

        int delivered = dispatcher.dispatchPending();

        assertThat(delivered).isEqualTo(1);
        assertThat(requestCount.get()).isEqualTo(1);
        assertThat(receivedEvents).containsExactly("DAG_RUN_CREATED");
        assertThat(receivedSignatures.get(0)).startsWith("sha256=");
        assertThat(receivedSignatures.get(0)).hasSizeGreaterThan(7);

        var saved = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(saved.getDeliveryStatus()).isEqualTo(OutboxEventEntity.STATUS_DISPATCHED);

        assertThat(dispatcher.dispatchPending()).isZero();
        assertThat(requestCount.get()).isEqualTo(1);
    }
}
