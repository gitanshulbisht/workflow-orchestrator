package com.buildathon.orchestrator.persistence;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TaskInstanceClaimTest extends AbstractIntegrationTest {

    @Autowired
    private DagRepository dagRepository;

    @Autowired
    private DagTaskRepository dagTaskRepository;

    @Autowired
    private DagRunRepository dagRunRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private UUID runId;
    private UUID taskId;

    @BeforeEach
    void setUp() {
        UUID dagId = UUID.randomUUID();
        dagRepository.saveAndFlush(new DagEntity(dagId, "claim-test", null, 1, null, "UTC", false,
                "name: claim-test", Instant.now(), Instant.now()));

        taskId = UUID.randomUUID();
        dagTaskRepository.saveAndFlush(new DagTaskEntity(taskId, dagId, "t1", "delay",
                "{\"seconds\":1}", 0, 5, 2.0, 60, false));

        runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.PENDING,
                "MANUAL", null, null, Instant.now()));

        UUID instanceId = UUID.randomUUID();
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(instanceId, runId, taskId,
                TaskState.SCHEDULED, Instant.now(), Instant.now(), null));
    }

    @AfterEach
    void tearDown() {
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
    }

    @Test
    void crudRoundTripsWork() {
        assertThat(dagRepository.findByName("claim-test")).isPresent();
        assertThat(dagTaskRepository.findByDagIdOrderByName(dagRepository.findByName("claim-test").get().getId()))
                .hasSize(1);
        assertThat(taskInstanceRepository.findByRunIdOrderByQueuedAt(runId)).hasSize(1);
    }

    @Test
    void twoParallelTransactionsNeverClaimTheSameTask() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch claimed = new CountDownLatch(1);

        Future<UUID> claim1 = pool.submit(() -> claimTask(start, claimed));
        Future<UUID> claim2 = pool.submit(() -> claimTask(start, claimed));

        start.countDown();
        UUID first = claim1.get(10, TimeUnit.SECONDS);
        UUID second = claim2.get(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        // Exactly one of the two competing workers may claim the task.
        assertThat((first != null) ^ (second != null))
                .as("expected exactly one successful claim, got first=%s second=%s", first, second)
                .isTrue();

        // After the first transaction commits, a second claim must also return null:
        // the row is no longer SCHEDULED (it was claimed).
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        List<TaskInstanceEntity> again = tx.execute(status ->
                taskInstanceRepository.claimNextDue(Instant.now().plusSeconds(1), 10));
        assertThat(again).isEmpty();
    }

    private UUID claimTask(CountDownLatch start, CountDownLatch claimed) throws Exception {
        start.await();
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        return tx.execute(status -> {
            List<TaskInstanceEntity> tasks =
                    taskInstanceRepository.claimNextDue(Instant.now().plusSeconds(1), 10);
            if (tasks.isEmpty()) {
                return null;
            }
            TaskInstanceEntity task = tasks.get(0);
            task.claim("worker-1", Instant.now());
            taskInstanceRepository.saveAndFlush(task);
            claimed.countDown();
            // Hold the row lock open until the other thread has attempted its claim.
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                status.setRollbackOnly();
                return null;
            }
            return task.getId();
        });
    }

    @Test
    void claimIgnoresTasksScheduledInTheFuture() {
        UUID instanceId = UUID.randomUUID();
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(instanceId, runId, taskId,
                TaskState.SCHEDULED, Instant.now(), Instant.now().plusSeconds(3600), null));

        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        List<TaskInstanceEntity> claims = tx.execute(status ->
                taskInstanceRepository.claimNextDue(Instant.now(), 10));

        assertThat(claims).isNotEmpty();
        assertThat(claims).allMatch(t -> !t.getId().equals(instanceId));
    }

    @Test
    void skipsNonScheduledStates() {
        UUID instanceId = UUID.randomUUID();
        taskInstanceRepository.saveAndFlush(new TaskInstanceEntity(instanceId, runId, taskId,
                TaskState.PENDING, Instant.now(), Instant.now(), null));

        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        List<TaskInstanceEntity> claims = tx.execute(status ->
                taskInstanceRepository.claimNextDue(Instant.now().plusSeconds(1), 10));

        assertThat(claims).allMatch(t -> !t.getId().equals(instanceId));
    }
}
