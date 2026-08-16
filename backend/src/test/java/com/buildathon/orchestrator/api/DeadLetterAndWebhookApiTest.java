package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.*;
import com.buildathon.orchestrator.service.DeadLetterService;
import com.buildathon.orchestrator.service.RunService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class DeadLetterAndWebhookApiTest extends AbstractIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DagRepository dagRepository;

    @Autowired
    private DagTaskRepository dagTaskRepository;

    @Autowired
    private DagRunRepository dagRunRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @Autowired
    private WebhookRepository webhookRepository;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private UUID dagId;
    private UUID taskId;
    private UUID runId;
    private UUID instanceId;
    private UUID dlId;

    @BeforeEach
    void setUp() {
        dagId = UUID.randomUUID();
        dagRepository.saveAndFlush(new DagEntity(dagId, "dlq-dag", null, 1, null, "UTC", false,
                "name: dlq-dag", Instant.now(), Instant.now()));
        taskId = UUID.randomUUID();
        dagTaskRepository.saveAndFlush(new DagTaskEntity(taskId, dagId, "dead-task", "fail",
                "{}", 0, 5, 2.0, 60, false));
        runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.RUNNING,
                "MANUAL", null, null, Instant.now()));
        instanceId = UUID.randomUUID();
        var instance = new TaskInstanceEntity(instanceId, runId, taskId, TaskState.DEAD_LETTERED,
                Instant.now(), Instant.now(), null);
        taskInstanceRepository.saveAndFlush(instance);
        dlId = UUID.randomUUID();
        deadLetterRepository.saveAndFlush(new DeadLetterEntity(dlId, instanceId, runId, "dead-task",
                "{\"error\":\"boom\"}", Instant.now()));
    }

    @AfterEach
    void tearDown() {
        webhookRepository.deleteAll();
        deadLetterRepository.deleteAll();
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
    }

    @Test
    void listsDeadLetters() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].taskName").value("dead-task"));
    }

    @Test
    void replayMovesTaskBackToScheduled() throws Exception {
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", dlId))
                .andExpect(status().isOk());

        var task = taskInstanceRepository.findById(instanceId).orElseThrow();
        assertThat(task.getState()).isEqualTo(TaskState.SCHEDULED);
        var dl = deadLetterRepository.findById(dlId).orElseThrow();
        assertThat(dl.getReplayStatus()).isEqualTo("REPLAYED");
    }

    @Test
    void webhookRegistrationAndListing() throws Exception {
        mockMvc.perform(post("/api/v1/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"url":"https://example.com/hook","secret":"s3cret","events":["DAG_RUN_CREATED"]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.url").value("https://example.com/hook"));

        mockMvc.perform(get("/api/v1/webhooks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    void replayIsIdempotent() throws Exception {
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", dlId))
                .andExpect(status().isOk());

        // Replaying again is a no-op (already replayed).
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", dlId))
                .andExpect(status().isOk());

        var dl = deadLetterRepository.findById(dlId).orElseThrow();
        assertThat(dl.getReplayStatus()).isEqualTo("REPLAYED");
    }
}
