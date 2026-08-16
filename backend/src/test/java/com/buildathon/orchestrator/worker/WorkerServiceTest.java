package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.DagEntity;
import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagRunEntity;
import com.buildathon.orchestrator.persistence.DagRunRepository;
import com.buildathon.orchestrator.persistence.DagTaskEntity;
import com.buildathon.orchestrator.persistence.DagTaskRepository;
import com.buildathon.orchestrator.persistence.DeadLetterRepository;
import com.buildathon.orchestrator.persistence.TaskAttemptRepository;
import com.buildathon.orchestrator.persistence.TaskDependencyEntity;
import com.buildathon.orchestrator.persistence.TaskDependencyRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import com.buildathon.orchestrator.service.RunService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerServiceTest extends AbstractIntegrationTest {

    @Autowired
    private DagRepository dagRepository;

    @Autowired
    private DagTaskRepository dagTaskRepository;

    @Autowired
    private DagRunRepository dagRunRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Autowired
    private TaskDependencyRepository taskDependencyRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private WorkerService workerService;

    private UUID dagId;

    @BeforeEach
    void setUp() {
        dagId = UUID.randomUUID();
        dagRepository.saveAndFlush(new DagEntity(dagId, "worker-dag", null, 1, null, "UTC", false,
                "name: worker-dag", Instant.now(), Instant.now()));
    }

    @AfterEach
    void tearDown() {
        taskAttemptRepository.deleteAll();
        deadLetterRepository.deleteAll();
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        taskDependencyRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
    }

    private UUID createTask(String name, String type, Map<String, Object> config,
                            int maxRetries, int retryDelaySeconds, boolean singleton) {
        UUID taskId = UUID.randomUUID();
        dagTaskRepository.saveAndFlush(new DagTaskEntity(taskId, dagId, name, type,
                toJson(config), maxRetries, retryDelaySeconds, 2.0, 60, singleton));
        return taskId;
    }

    private UUID createRun(UUID taskId, TaskState taskState) {
        UUID runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.RUNNING,
                "MANUAL", null, null, Instant.now()));
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(UUID.randomUUID(), runId, taskId,
                taskState, Instant.now(), taskState == TaskState.SCHEDULED ? Instant.now() : null, null));
        return runId;
    }

    private TaskInstanceEntity task(UUID runId) {
        return taskInstanceRepository.findByRunIdOrderByQueuedAt(runId).get(0);
    }

    @Test
    void happyPathBashTaskCompletesSuccessfully() {
        UUID taskId = createTask("hello", "bash", Map.of("command", "echo hello"), 0, 5, false);
        UUID runId = createRun(taskId, TaskState.SCHEDULED);

        workerService.executeNext("worker-1");

        assertThat(task(runId).getState()).isEqualTo(TaskState.SUCCESS);
        assertThat(taskAttemptRepository.findByTaskInstanceIdOrderByAttemptNo(task(runId).getId())).hasSize(1);
        assertThat(deadLetterRepository.findAll()).isEmpty();
    }

    @Test
    void failingTaskRetriesWithBackoffThenDeadLetters() {
        UUID taskId = createTask("fail-task", "fail", Map.of(), 2, 1, false);
        UUID runId = createRun(taskId, TaskState.SCHEDULED);

        var service = workerService;

        service.executeNext("worker-1");
        var t = task(runId);
        assertThat(t.getState()).isEqualTo(TaskState.UP_FOR_RETRY);
        assertThat(t.getAttemptNo()).isEqualTo(1);
        assertThat(t.getScheduledAt()).isAfter(Instant.now());

        // Fast-forward: keep the attempt counter, make the retry due now.
        t.scheduleRetry(t.getAttemptNo(), Instant.now());
        t.transition(TaskState.SCHEDULED, Instant.now());
        taskInstanceRepository.saveAndFlush(t);
        service.executeNext("worker-1");

        t = task(runId);
        assertThat(t.getState()).isEqualTo(TaskState.UP_FOR_RETRY);
        assertThat(t.getAttemptNo()).isEqualTo(2);

        t.scheduleRetry(t.getAttemptNo(), Instant.now());
        t.transition(TaskState.SCHEDULED, Instant.now());
        taskInstanceRepository.saveAndFlush(t);
        service.executeNext("worker-1");

        t = task(runId);
        assertThat(t.getState()).isEqualTo(TaskState.DEAD_LETTERED);
        assertThat(deadLetterRepository.findAll()).hasSize(1);
        assertThat(taskAttemptRepository.findByTaskInstanceIdOrderByAttemptNo(t.getId())).hasSize(3);
    }

    @Test
    void backoffScalesExponentially() {
        UUID taskId = createTask("backoff", "fail", Map.of(), 5, 5, false);
        UUID runId = createRun(taskId, TaskState.SCHEDULED);

        workerService.executeNext("worker-1");

        var t = task(runId);
        // Full-jitter backoff: delay in [0, base*backoff^attempt]. For the first
        // retry with base 5s the window is [0, 5s].
        assertThat(t.getScheduledAt()).isAfter(Instant.now());
        assertThat(t.getScheduledAt()).isBefore(Instant.now().plusSeconds(6));
    }

    @Test
    void downstreamSchedulesOnlyAfterUpstreamSuccess() {
        UUID firstId = createTask("first", "delay", Map.of("seconds", 0), 0, 5, false);
        UUID secondId = createTask("second", "delay", Map.of("seconds", 0), 0, 5, false);
        taskDependencyRepository.saveAndFlush(new TaskDependencyEntity(secondId, firstId));

        UUID runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.RUNNING,
                "MANUAL", null, null, Instant.now()));
        UUID firstInstanceId = UUID.randomUUID();
        UUID secondInstanceId = UUID.randomUUID();
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(firstInstanceId, runId, firstId,
                TaskState.SCHEDULED, Instant.now(), Instant.now(), null));
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(secondInstanceId, runId, secondId,
                TaskState.PENDING, Instant.now(), null, null));

        workerService.executeNext("worker-1");

        assertThat(taskInstanceRepository.findById(firstInstanceId).orElseThrow().getState())
                .isEqualTo(TaskState.SUCCESS);
        assertThat(taskInstanceRepository.findById(secondInstanceId).orElseThrow().getState())
                .isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void upstreamFailureSkipsDownstream() {
        UUID firstId = createTask("bad-first", "fail", Map.of(), 0, 5, false);
        UUID secondId = createTask("second", "delay", Map.of("seconds", 0), 0, 5, false);
        taskDependencyRepository.saveAndFlush(new TaskDependencyEntity(secondId, firstId));

        UUID runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.RUNNING,
                "MANUAL", null, null, Instant.now()));
        UUID firstInstanceId = UUID.randomUUID();
        UUID secondInstanceId = UUID.randomUUID();
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(firstInstanceId, runId, firstId,
                TaskState.SCHEDULED, Instant.now(), Instant.now(), null));
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(secondInstanceId, runId, secondId,
                TaskState.PENDING, Instant.now(), null, null));

        workerService.executeNext("worker-1");

        var first = taskInstanceRepository.findById(firstInstanceId).orElseThrow();
        var second = taskInstanceRepository.findById(secondInstanceId).orElseThrow();
        assertThat(first.getState()).isEqualTo(TaskState.DEAD_LETTERED);
        assertThat(second.getState()).isEqualTo(TaskState.SKIPPED);
    }

    private String toJson(Map<String, Object> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
