package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.outbox.SseBroadcaster;
import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagRunRepository;
import com.buildathon.orchestrator.persistence.OutboxEventRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class RunApiIntegrationTest extends AbstractIntegrationTest {

    private static final String DAG_YAML = """
            name: run-test-dag
            tasks:
              - name: a
                type: delay
                config:
                  seconds: 0
              - name: b
                type: delay
                config:
                  seconds: 0
                dependsOn: [a]
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DagRepository dagRepository;

    @Autowired
    private DagRunRepository dagRunRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private SseBroadcaster sseBroadcaster;

    @Autowired
    private com.buildathon.orchestrator.service.RunService runService;

    @Autowired
    private ObjectMapper objectMapper;

    private UUID dagId;

    @BeforeEach
    void setUp() throws Exception {
        String created = mockMvc.perform(post("/api/v1/dags")
                        .contentType("application/yaml")
                        .content(DAG_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        dagId = UUID.fromString(objectMapper.readTree(created).get("id").asText());
    }

    @AfterEach
    void tearDown() {
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        outboxEventRepository.deleteAll();
        dagRepository.deleteAll();
    }

    @Test
    void triggerCreatesRunWithScheduledRootTasksAndOutboxEvents() throws Exception {
        mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.state").value("PENDING"))
                .andExpect(jsonPath("$.triggerType").value("MANUAL"));

        var run = dagRunRepository.findAll().get(0);
        assertThat(run.getDagId()).isEqualTo(dagId);

        var tasks = taskInstanceRepository.findByRunIdOrderByQueuedAt(run.getId());
        assertThat(tasks).hasSize(2);
        var root = tasks.stream().filter(t -> t.getState() == TaskState.SCHEDULED).toList();
        assertThat(root).hasSize(1);
        assertThat(root.get(0).getScheduledAt()).isNotNull();

        assertThat(outboxEventRepository.findAll())
                .anyMatch(e -> e.getEventType().equals("DAG_RUN_CREATED"))
                .anyMatch(e -> e.getEventType().equals("TASK_INSTANCE_SCHEDULED"));
    }

    @Test
    void idempotentTriggerReturnsSameRun() throws Exception {
        String first = mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId)
                        .header("Idempotency-Key", "run-key-1"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String second = mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId)
                        .header("Idempotency-Key", "run-key-1"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        assertThat(objectMapper.readTree(first).get("id"))
                .isEqualTo(objectMapper.readTree(second).get("id"));
        assertThat(dagRunRepository.count()).isEqualTo(1);
    }

    @Test
    void cancellingRunCancelsPendingAndScheduledTasks() throws Exception {
        String runJson = mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID runId = UUID.fromString(objectMapper.readTree(runJson).get("id").asText());

        mockMvc.perform(post("/api/v1/dags/{id}/runs/{runId}/cancel", dagId, runId))
                .andExpect(status().isOk());

        assertThat(dagRunRepository.findById(runId).orElseThrow().getState()).isEqualTo(RunState.CANCELLED);
        assertThat(taskInstanceRepository.findByRunIdOrderByQueuedAt(runId))
                .allMatch(t -> t.getState() == TaskState.CANCELLED);
    }

    @Test
    void finalizeRunMarksSuccessWhenAllTasksSucceed() throws Exception {
        String runJson = mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID runId = UUID.fromString(objectMapper.readTree(runJson).get("id").asText());

        var run = dagRunRepository.findById(runId).orElseThrow();
        run.transition(RunState.RUNNING, java.time.Instant.now());
        dagRunRepository.saveAndFlush(run);

        for (var task : taskInstanceRepository.findByRunIdOrderByQueuedAt(runId)) {
            if (task.getState() == TaskState.PENDING) {
                task.transition(TaskState.SCHEDULED, java.time.Instant.now());
            }
            if (task.getState() == TaskState.SCHEDULED) {
                task.transition(TaskState.RUNNING, java.time.Instant.now());
            }
            task.transition(TaskState.SUCCESS, java.time.Instant.now());
            taskInstanceRepository.saveAndFlush(task);
        }
        runService.finalizeRunIfComplete(runId);

        assertThat(dagRunRepository.findById(runId).orElseThrow().getState()).isEqualTo(RunState.SUCCESS);
    }

    @Test
    void outboxRelayPublishesAndMarksEvents() throws Exception {
        mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId))
                .andExpect(status().isCreated());

        var relay = new com.buildathon.orchestrator.outbox.OutboxRelay(outboxEventRepository,
                event -> {
                    // no-op publisher for this test
                });
        relay.publishPending();

        assertThat(outboxEventRepository.findAll()).isNotEmpty()
                .allMatch(e -> e.getDeliveryStatus().equals("PUBLISHED"));
    }

    @Test
    void sseBroadcasterReceivesEvents() throws Exception {
        java.util.List<String> received = new java.util.concurrent.CopyOnWriteArrayList<>();
        sseBroadcaster.register(new SseEmitter() {
            @Override
            public void send(SseEventBuilder builder) throws java.io.IOException {
                received.add(builder.build().stream()
                        .filter(d -> d.getData() != null)
                        .map(d -> d.getData().toString())
                        .findFirst().orElse(""));
            }
        });

        mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId))
                .andExpect(status().isCreated());

        var relay = new com.buildathon.orchestrator.outbox.OutboxRelay(outboxEventRepository,
                event -> sseBroadcaster.broadcast(event.getEventType(), event.getPayload()));
        relay.publishPending();

        assertThat(received).anyMatch(s -> s.contains("DAG_RUN_CREATED"));
    }

    @Test
    void runEndpointsListAndFilter() throws Exception {
        mockMvc.perform(post("/api/v1/dags/{id}/runs", dagId)).andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/runs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1));

        mockMvc.perform(get("/api/v1/runs").param("dagId", dagId.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }
}
