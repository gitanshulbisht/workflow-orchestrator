package com.buildathon.orchestrator.scheduling;

import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.domain.TaskStateMachine;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.DagRunRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

/**
 * Recovers from worker crashes: marks RUNNING tasks with stale heartbeats as
 * FAILED (they re-enter the retry path) and fails runs whose workers died.
 */
@Component
public class ReaperService {

    private static final Logger log = LoggerFactory.getLogger(ReaperService.class);

    private final TaskInstanceRepository taskInstanceRepository;
    private final DagRunRepository dagRunRepository;
    private final OutboxWriter outboxWriter;

    public ReaperService(TaskInstanceRepository taskInstanceRepository,
                         DagRunRepository dagRunRepository,
                         OutboxWriter outboxWriter) {
        this.taskInstanceRepository = taskInstanceRepository;
        this.dagRunRepository = dagRunRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public int reapStaleTasks(Instant now, long staleHeartbeatMs) {
        Instant staleBefore = now.minusMillis(staleHeartbeatMs);
        var stale = taskInstanceRepository.findStaleRunning(staleBefore);
        int reaped = 0;
        for (var task : stale) {
            task.markError("Worker heartbeat lost (stale) — recovered by reaper");
            task.transition(TaskState.FAILED, now);
            taskInstanceRepository.save(task);
            if (outboxWriter != null) {
                outboxWriter.write("TASK_INSTANCE", task.getId().toString(), "TASK_INSTANCE_FAILED",
                        Map.of("runId", task.getRunId().toString(),
                                "taskInstanceId", task.getId().toString(),
                                "reason", "stale-heartbeat"));
            }
            reaped++;
            log.warn("Reaped stale task {} (run {})", task.getId(), task.getRunId());
        }
        return reaped;
    }

    /**
     * Safety net: finalizes any RUNNING run whose tasks are all terminal.
     * Normally the worker's propagation path does this; this sweep heals the
     * rare case where a concurrent finalize lost an optimistic-lock race.
     */
    @Transactional
    public int finalizeCompletedRuns() {
        var running = dagRunRepository.findStaleRunning(RunState.RUNNING, Instant.now().plusSeconds(86400));
        int finalized = 0;
        for (var run : running) {
            var instances = taskInstanceRepository.findByRunIdOrderByQueuedAt(run.getId());
            boolean allTerminal = instances.stream().allMatch(t -> TaskStateMachine.isTerminal(t.getState()));
            if (!allTerminal || instances.isEmpty()) {
                continue;
            }
            boolean anyFailed = instances.stream()
                    .anyMatch(t -> t.getState() == TaskState.FAILED || t.getState() == TaskState.DEAD_LETTERED);
            RunState finalState = anyFailed ? RunState.FAILED : RunState.SUCCESS;
            Instant now = Instant.now();
            run.transition(finalState, now);
            dagRunRepository.save(run);
            if (outboxWriter != null) {
                outboxWriter.write("DAG_RUN", run.getId().toString(), "DAG_RUN_" + finalState.name(),
                        Map.of("runId", run.getId().toString(), "dagId", run.getDagId().toString(),
                                "state", finalState.name(), "reason", "reaper-sweep"));
            }
            finalized++;
            log.info("Reaper finalized run {} as {}", run.getId(), finalState);
        }
        return finalized;
    }
}
