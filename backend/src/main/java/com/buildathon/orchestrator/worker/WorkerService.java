package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import com.buildathon.orchestrator.domain.BackoffCalculator;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.lock.LockManager;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.DagTaskEntity;
import com.buildathon.orchestrator.persistence.DagTaskRepository;
import com.buildathon.orchestrator.persistence.DeadLetterEntity;
import com.buildathon.orchestrator.persistence.DeadLetterRepository;
import com.buildathon.orchestrator.persistence.TaskAttemptEntity;
import com.buildathon.orchestrator.persistence.TaskAttemptRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import com.buildathon.orchestrator.service.RunService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Executes a claimed task: run the executor, record the attempt, decide
 * retry (exponential backoff with jitter), dead-letter on exhaustion, and
 * propagate downstream scheduling. Each step is transactional.
 */
@Service
public class WorkerService {

    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final DagTaskRepository dagTaskRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final OutboxWriter outboxWriter;
    private final LockManager lockManager;
    private final PlatformTransactionManager transactionManager;
    private final RunService runService;
    private final ObjectMapper objectMapper;
    private final ExecutorRegistry executorRegistry;
    private final OrchestratorProperties properties;

    public WorkerService(TaskInstanceRepository taskInstanceRepository, TaskAttemptRepository taskAttemptRepository,
                         DagTaskRepository dagTaskRepository, DeadLetterRepository deadLetterRepository,
                         OutboxWriter outboxWriter, LockManager lockManager,
                         PlatformTransactionManager transactionManager, RunService runService,
                         ObjectMapper objectMapper, ExecutorRegistry executorRegistry,
                         OrchestratorProperties properties) {
        this.taskInstanceRepository = taskInstanceRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.dagTaskRepository = dagTaskRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.outboxWriter = outboxWriter;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.runService = runService;
        this.objectMapper = objectMapper;
        this.executorRegistry = executorRegistry;
        this.properties = properties;
    }

    /**
     * Claims the next due task (SKIP LOCKED) and executes it. Returns the
     * instance id if a task was executed, else null.
     */
    public UUID executeNext(String workerId) {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        TaskInstanceEntity claimed = tx.execute(status -> {
            List<TaskInstanceEntity> tasks = taskInstanceRepository.claimNextDue(Instant.now(), 1);
            if (tasks.isEmpty()) {
                return null;
            }
            TaskInstanceEntity task = tasks.get(0);
            task.claim(workerId, Instant.now());
            taskInstanceRepository.save(task);
            return task;
        });
        if (claimed == null) {
            return null;
        }
        doExecute(claimed.getId(), claimed.getDagTaskId(), workerId);
        return claimed.getId();
    }

    /**
     * Re-arms tasks whose retry window has elapsed: UP_FOR_RETRY → SCHEDULED.
     * Called from the worker poll loop so retry timing stays worker-driven.
     * Optimistic-lock conflicts are expected (another worker claimed or
     * re-armed the row first) and are simply skipped.
     */
    public int scheduleDueRetries() {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        Integer rearmed = tx.execute(status -> {
            List<TaskInstanceEntity> due = taskInstanceRepository.findDueRetries(Instant.now());
            int count = 0;
            for (TaskInstanceEntity task : due) {
                try {
                    task.transition(TaskState.SCHEDULED, Instant.now());
                    taskInstanceRepository.saveAndFlush(task);
                    count++;
                } catch (org.springframework.dao.OptimisticLockingFailureException e) {
                    log.debug("Lost retry re-arm race for task {} — skipping", task.getId());
                }
            }
            return count;
        });
        return rearmed == null ? 0 : rearmed;
    }

    /**
     * Executes an already-claimed task, resolving the executor from the task type.
     */
    public void executeClaimed(UUID instanceId, String workerId) {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        UUID dagTaskId = tx.execute(status -> {
            TaskInstanceEntity task = taskInstanceRepository.findById(instanceId).orElseThrow();
            if (task.getState() != TaskState.RUNNING) {
                return null;
            }
            return task.getDagTaskId();
        });
        if (dagTaskId != null) {
            doExecute(instanceId, dagTaskId, workerId);
        }
    }

    public void executeClaimed(UUID instanceId, TaskExecutor executor, String workerId) {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        UUID dagTaskId = tx.execute(status -> {
            TaskInstanceEntity task = taskInstanceRepository.findById(instanceId).orElseThrow();
            return task.getDagTaskId();
        });
        doExecuteWithExecutor(instanceId, dagTaskId, executor, workerId);
    }

    private void doExecute(UUID instanceId, UUID dagTaskId, String workerId) {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        String type = tx.execute(status ->
                dagTaskRepository.findById(dagTaskId).orElseThrow().getTaskType());
        doExecuteWithExecutor(instanceId, dagTaskId, executorRegistry.forType(type), workerId);
    }

    private void doExecuteWithExecutor(UUID instanceId, UUID dagTaskId, TaskExecutor executor, String workerId) {
        // Fetch spec/attempt data in a short transaction, run the executor
        // OUTSIDE any transaction (so the heartbeater can update the row
        // without contending with the worker's lock), then finalize in a new
        // transaction.
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        Integer attemptNo = tx.execute(status -> {
            TaskInstanceEntity task = taskInstanceRepository.findById(instanceId).orElseThrow();
            if (task.getState() != TaskState.RUNNING) {
                return null;
            }
            DagTaskEntity spec = dagTaskRepository.findById(dagTaskId).orElseThrow();
            int no = task.getAttemptNo() + 1;
            Instant startedAt = Instant.now();
            TaskAttemptEntity attempt = new TaskAttemptEntity(UUID.randomUUID(), task.getId(), no, "RUNNING", startedAt);
            taskAttemptRepository.saveAndFlush(attempt);
            task.heartbeat(startedAt);
            taskInstanceRepository.saveAndFlush(task);
            return no;
        });
        if (attemptNo == null) {
            return;
        }

        DagTaskEntity spec = tx.execute(status -> dagTaskRepository.findById(dagTaskId).orElseThrow());
        Map<String, Object> config = parseConfig(spec.getConfig());

        TaskExecutionResult result;
        Thread heartbeater = startHeartbeater(instanceId);
        try {
            result = executeWithSingletonGuard(instanceId, spec, executor, config);
        } catch (Exception e) {
            result = TaskExecutionResult.failure(e.getMessage(), null);
        } finally {
            stopHeartbeater(heartbeater);
        }

        final TaskExecutionResult outcome = result;
        tx.executeWithoutResult(status -> {
            TaskInstanceEntity task = taskInstanceRepository.findById(instanceId).orElseThrow();
            Instant endedAt = Instant.now();
            TaskAttemptEntity attempt = taskAttemptRepository
                    .findByTaskInstanceIdAndAttemptNo(task.getId(), attemptNo)
                    .orElse(null);
            if (attempt != null) {
                attempt.complete(outcome.isSuccess() ? "SUCCESS" : "FAILED", endedAt,
                        outcome.exitCode(), outcome.logTail(), outcome.error());
                taskAttemptRepository.save(attempt);
            }

            if (outcome.isSuccess()) {
                task.transition(TaskState.SUCCESS, endedAt);
                taskInstanceRepository.saveAndFlush(task);
                outboxWriter.write("TASK_INSTANCE", task.getId().toString(), "TASK_INSTANCE_SUCCEEDED",
                        Map.of("runId", task.getRunId().toString(), "taskInstanceId", task.getId().toString(),
                                "attempt", attemptNo));
                propagateDownstream(task.getRunId());
                return;
            }

            task.markError(outcome.error());
            if (attemptNo <= spec.getMaxRetries()) {
                task.transition(TaskState.FAILED, endedAt);
                task.transition(TaskState.UP_FOR_RETRY, endedAt);
                long delayMillis = BackoffCalculator.computeDelayMillis(
                        spec.getRetryDelaySeconds(), spec.getRetryBackoff(), attemptNo);
                Instant nextAttempt = Instant.now().plusMillis(delayMillis);
                task.scheduleRetry(attemptNo, nextAttempt);
                taskInstanceRepository.saveAndFlush(task);
                outboxWriter.write("TASK_INSTANCE", task.getId().toString(), "TASK_INSTANCE_RETRY_SCHEDULED",
                        Map.of("runId", task.getRunId().toString(), "taskInstanceId", task.getId().toString(),
                                "attempt", attemptNo, "nextAttemptAt", nextAttempt.toString(),
                                "delayMillis", delayMillis));
                log.info("Task {} attempt {} failed; retry scheduled in {}ms", task.getId(), attemptNo, delayMillis);
            } else {
                task.transition(TaskState.FAILED, endedAt);
                task.transition(TaskState.DEAD_LETTERED, endedAt);
                task.scheduleRetry(attemptNo, null);
                taskInstanceRepository.saveAndFlush(task);
                // Dedup: a concurrent re-arm can't resurrect a dead-lettered task
                // anymore (optimistic locking), but keep the DLQ row unique per
                // task instance regardless.
                if (deadLetterRepository.findByTaskInstanceId(task.getId()).isEmpty()) {
                    DeadLetterEntity dl = new DeadLetterEntity(UUID.randomUUID(), task.getId(), task.getRunId(),
                            spec.getName(), toJson(Map.of(
                                    "error", outcome.error() == null ? "unknown" : outcome.error(),
                                    "attempts", attemptNo,
                                    "logTail", outcome.logTail() == null ? "" : outcome.logTail())), endedAt);
                    deadLetterRepository.save(dl);
                    outboxWriter.write("TASK_INSTANCE", task.getId().toString(), "TASK_INSTANCE_DEAD_LETTERED",
                            Map.of("runId", task.getRunId().toString(), "taskInstanceId", task.getId().toString(),
                                    "deadLetterId", dl.getId().toString(), "attempts", attemptNo));
                    log.warn("Task {} dead-lettered after {} attempts", task.getId(), attemptNo);
                }
                propagateDownstream(task.getRunId());
            }
        });
    }

    private Thread startHeartbeater(UUID instanceId) {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(properties.worker().heartbeatIntervalMs());
                    TransactionTemplate tx = new TransactionTemplate(transactionManager);
                    tx.executeWithoutResult(status ->
                            taskInstanceRepository.refreshHeartbeat(instanceId, Instant.now()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    log.debug("Heartbeat refresh failed for task {}: {}", instanceId, e.getMessage());
                }
            }
        }, "heartbeat-" + instanceId);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private void stopHeartbeater(Thread heartbeater) {
        if (heartbeater != null) {
            heartbeater.interrupt();
        }
    }

    private TaskExecutionResult executeWithSingletonGuard(UUID instanceId, DagTaskEntity spec,
                                                          TaskExecutor executor, Map<String, Object> config)
            throws Exception {
        TaskInstanceEntity task = new TransactionTemplate(transactionManager).execute(status ->
                taskInstanceRepository.findById(instanceId).orElseThrow());
        if (!spec.isSingleton() || lockManager == null) {
            return executor.execute(task, config, spec.getTimeoutSeconds());
        }
        var holder = new Object() {
            TaskExecutionResult r;
        };
        lockManager.withLock("singleton:" + spec.getDagId() + ":" + spec.getId(), 0, () -> {
            try {
                holder.r = executor.execute(task, config, spec.getTimeoutSeconds());
            } catch (Exception e) {
                holder.r = TaskExecutionResult.failure(e.getMessage(), null);
            }
            return null;
        });
        if (holder.r == null) {
            throw new SingletonLockBusyException();
        }
        return holder.r;
    }

    private void propagateDownstream(UUID runId) {
        try {
            runService.propagate(runId);
        } catch (Exception e) {
            log.error("Failed to propagate downstream tasks for run {}", runId, e);
        }
    }

    private Map<String, Object> parseConfig(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            return Map.of();
        }
    }

    private String toJson(Map<String, Object> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            return "{}";
        }
    }

    public static class SingletonLockBusyException extends RuntimeException {
        public SingletonLockBusyException() {
            super("singleton task lock is held by another worker");
        }
    }
}
