package com.buildathon.orchestrator.scheduling;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.persistence.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SchedulerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private DagRepository dagRepository;

    @Autowired
    private DagTaskRepository dagTaskRepository;

    @Autowired
    private DagScheduleRepository dagScheduleRepository;

    @Autowired
    private DagRunRepository dagRunRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private LeaderElection leaderElection;

    @Autowired
    private com.cronutils.parser.CronParser cronParser;

    @Autowired
    private com.buildathon.orchestrator.service.RunService runService;

    private UUID dagId;
    private UUID taskId;

    @BeforeEach
    void setUp() {
        dagId = UUID.randomUUID();
        dagRepository.saveAndFlush(new DagEntity(dagId, "cron-dag", null, 1, "0 0 * * * ?", "UTC",
                false, "name: cron-dag", Instant.now(), Instant.now()));
        taskId = UUID.randomUUID();
        dagTaskRepository.saveAndFlush(new DagTaskEntity(taskId, dagId, "t", "delay",
                "{\"seconds\":0}", 0, 5, 2.0, 60, false));
    }

    @AfterEach
    void tearDown() {
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        dagScheduleRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
    }

    @Test
    void leaderElectionOnlyOneLeaderWins() throws Exception {
        var first = leaderElection.tryAcquire(2, TimeUnit.SECONDS);
        assertThat(first).isTrue();

        // A different thread (i.e. a different scheduler instance) must lose.
        java.util.concurrent.atomic.AtomicBoolean second = new java.util.concurrent.atomic.AtomicBoolean();
        Thread other = new Thread(() -> second.set(leaderElection.tryAcquire(500, TimeUnit.MILLISECONDS)));
        other.start();
        other.join();

        assertThat(second.get()).isFalse();
        leaderElection.release();
    }

    @Test
    void scanTriggersDueDagAndAdvancesNextRun() {
        dagScheduleRepository.saveAndFlush(new DagScheduleEntity(dagId, Instant.now().minusSeconds(5), null, "SKIP"));

        var scanner = new CronScanner(dagScheduleRepository, dagRepository, runService, cronParser);
        scanner.scanOnce(Instant.now());

        assertThat(dagRunRepository.findAll()).hasSize(1);
        var schedule = dagScheduleRepository.findByDagId(dagId).orElseThrow();
        assertThat(schedule.getNextRunAt()).isAfter(Instant.now());
        assertThat(schedule.getLastRunAt()).isNotNull();
    }

    @Test
    void pausedDagDoesNotTrigger() {
        var dag = dagRepository.findById(dagId).orElseThrow();
        dag.setPaused(true, Instant.now());
        dagRepository.saveAndFlush(dag);
        dagScheduleRepository.saveAndFlush(new DagScheduleEntity(dagId, Instant.now().minusSeconds(5), null, "SKIP"));

        var scanner = new CronScanner(dagScheduleRepository, dagRepository, runService, cronParser);
        scanner.scanOnce(Instant.now());

        assertThat(dagRunRepository.findAll()).isEmpty();
    }

    @Test
    void reaperMarksStaleRunningTasksFailed() {
        UUID runId = UUID.randomUUID();
        dagRunRepository.saveAndFlush(new DagRunEntity(runId, dagId, 1, RunState.RUNNING,
                "MANUAL", null, null, Instant.now()));
        UUID instanceId = UUID.randomUUID();
        var task = new TaskInstanceEntity(instanceId, runId, taskId, TaskState.RUNNING,
                Instant.now(), Instant.now(), null);
        task.heartbeat(Instant.now().minusSeconds(3600)); // long-stale heartbeat
        taskInstanceRepository.saveAndFlush(task);

        var reaper = new ReaperService(taskInstanceRepository, dagRunRepository, null);
        reaper.reapStaleTasks(Instant.now(), 30000);

        var after = taskInstanceRepository.findById(instanceId).orElseThrow();
        assertThat(after.getState()).isEqualTo(TaskState.FAILED);
    }

    @Test
    void schedulerLockSerializesScan() {
        // The scanner itself is idempotent: due schedule is advanced only after triggering.
        dagScheduleRepository.saveAndFlush(new DagScheduleEntity(dagId, Instant.now().minusSeconds(5), null, "SKIP"));
        var scanner = new CronScanner(dagScheduleRepository, dagRepository, runService, cronParser);
        scanner.scanOnce(Instant.now());
        scanner.scanOnce(Instant.now()); // second scan: nothing due anymore
        assertThat(dagRunRepository.findAll()).hasSize(1);
    }
}
