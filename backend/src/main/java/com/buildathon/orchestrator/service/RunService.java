package com.buildathon.orchestrator.service;

import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.RunStateMachine;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.domain.TaskStateMachine;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.DagEntity;
import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagRunEntity;
import com.buildathon.orchestrator.persistence.DagRunRepository;
import com.buildathon.orchestrator.persistence.DagTaskEntity;
import com.buildathon.orchestrator.persistence.DagTaskRepository;
import com.buildathon.orchestrator.persistence.TaskDependencyEntity;
import com.buildathon.orchestrator.persistence.TaskDependencyRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import java.util.UUID;

/**
 * Orchestrates DAG runs: triggering, task scheduling, completion detection,
 * cancellation. All mutations are transactional and write outbox events in
 * the same transaction as the state change.
 */
@Service
public class RunService {

    private static final Logger log = LoggerFactory.getLogger(RunService.class);

    public static final int MAX_CONCURRENT_RUNS_PER_DAG = 10;

    private final DagRepository dagRepository;
    private final DagRunRepository dagRunRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final DagTaskRepository dagTaskRepository;
    private final TaskDependencyRepository taskDependencyRepository;
    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;

    public RunService(DagRepository dagRepository, DagRunRepository dagRunRepository,
                      TaskInstanceRepository taskInstanceRepository, DagTaskRepository dagTaskRepository,
                      TaskDependencyRepository taskDependencyRepository, OutboxWriter outboxWriter,
                      ObjectMapper objectMapper) {
        this.dagRepository = dagRepository;
        this.dagRunRepository = dagRunRepository;
        this.taskInstanceRepository = taskInstanceRepository;
        this.dagTaskRepository = dagTaskRepository;
        this.taskDependencyRepository = taskDependencyRepository;
        this.outboxWriter = outboxWriter;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public DagRunEntity trigger(UUID dagId, String triggerType, Map<String, Object> payload, String idempotencyKey) {
        if (idempotencyKey != null) {
            var existing = dagRunRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return existing.get();
            }
        }
        DagEntity dag = dagRepository.findById(dagId)
                .orElseThrow(() -> new DagService.NotFoundException("DAG not found: " + dagId));
        if (dag.isPaused()) {
            throw new DagService.ConflictException("DAG '" + dag.getName() + "' is paused");
        }
        long activeRuns = dagRunRepository.countByDagIdAndStateIn(dagId,
                java.util.List.of(RunState.PENDING, RunState.RUNNING));
        if (activeRuns >= MAX_CONCURRENT_RUNS_PER_DAG) {
            throw new DagService.ConflictException(
                    "DAG '" + dag.getName() + "' already has " + activeRuns + " active runs (max " + MAX_CONCURRENT_RUNS_PER_DAG + ")");
        }

        Instant now = Instant.now();
        DagRunEntity run = new DagRunEntity(UUID.randomUUID(), dagId, dag.getVersion(), RunState.PENDING,
                triggerType, toJson(payload), idempotencyKey, now);
        dagRunRepository.saveAndFlush(run);

        // Create task instances. Root tasks (no dependencies) start SCHEDULED.
        List<DagTaskEntity> tasks = dagTaskRepository.findByDagIdAndVersionOrderByName(dagId, dag.getVersion());
        Map<UUID, Boolean> hasDeps = new HashMap<>();
        for (DagTaskEntity task : tasks) {
            hasDeps.put(task.getId(), !taskDependencyRepository.findByIdTaskId(task.getId()).isEmpty());
        }
        for (DagTaskEntity task : tasks) {
            TaskState initialState = hasDeps.get(task.getId()) ? TaskState.PENDING : TaskState.SCHEDULED;
            TaskInstanceEntity instance = new TaskInstanceEntity(UUID.randomUUID(), run.getId(), task.getId(),
                    initialState, now, initialState == TaskState.SCHEDULED ? now : null, null);
            taskInstanceRepository.save(instance);
            outboxWriter.write("TASK_INSTANCE", instance.getId().toString(), "TASK_INSTANCE_SCHEDULED",
                    Map.of("runId", run.getId().toString(), "taskId", task.getId().toString(),
                            "taskName", task.getName(), "state", initialState.name()));
        }

        outboxWriter.write("DAG_RUN", run.getId().toString(), "DAG_RUN_CREATED",
                Map.of("dagId", dagId.toString(), "runId", run.getId().toString(),
                        "triggerType", triggerType, "state", run.getState().name()));
        log.info("Triggered DAG run {} for DAG {}", run.getId(), dag.getName());
        return run;
    }

    @Transactional
    public void markStarted(UUID runId) {
        DagRunEntity run = dagRunRepository.findById(runId).orElseThrow();
        if (run.getState() == RunState.PENDING) {
            run.transition(RunState.RUNNING, Instant.now());
            dagRunRepository.save(run);
            outboxWriter.write("DAG_RUN", runId.toString(), "DAG_RUN_STARTED",
                    Map.of("runId", runId.toString(), "dagId", run.getDagId().toString()));
        }
    }

    /**
     * After a task instance reaches a terminal state, schedule any now-ready
     * downstream tasks and skip any doomed ones. Called in the same transaction
     * as the state change.
     */
    @Transactional
    public void propagate(UUID runId) {
        DagRunEntity run = dagRunRepository.findById(runId).orElseThrow();
        List<TaskInstanceEntity> instances = taskInstanceRepository.findByRunIdOrderByQueuedAt(runId);
        Map<UUID, TaskInstanceEntity> byTaskId = new HashMap<>();
        for (TaskInstanceEntity instance : instances) {
            byTaskId.put(instance.getDagTaskId(), instance);
        }

        boolean changed;
        do {
            changed = false;
            for (TaskInstanceEntity instance : byTaskId.values()) {
                if (instance.getState() != TaskState.PENDING) {
                    continue;
                }
                List<UUID> depTaskIds = taskDependencyRepository.findByIdTaskId(instance.getDagTaskId())
                        .stream().map(TaskDependencyEntity::getDependsOnTaskId).toList();
                boolean allSuccess = true;
                boolean anyFailed = false;
                for (UUID depTaskId : depTaskIds) {
                    TaskInstanceEntity dep = byTaskId.get(depTaskId);
                    if (dep == null) {
                        anyFailed = true;
                        break;
                    }
                    if (dep.getState() == TaskState.SUCCESS) {
                        continue;
                    }
                    allSuccess = false;
                    if (dep.getState() == TaskState.FAILED || dep.getState() == TaskState.DEAD_LETTERED
                            || dep.getState() == TaskState.SKIPPED || dep.getState() == TaskState.CANCELLED) {
                        anyFailed = true;
                    }
                }
                Instant now = Instant.now();
                if (allSuccess) {
                    instance.transition(TaskState.SCHEDULED, now);
                    taskInstanceRepository.save(instance);
                    outboxWriter.write("TASK_INSTANCE", instance.getId().toString(), "TASK_INSTANCE_SCHEDULED",
                            Map.of("runId", runId.toString(), "taskName", taskName(instance.getDagTaskId()),
                                    "state", instance.getState().name()));
                    changed = true;
                } else if (anyFailed) {
                    instance.transition(TaskState.SKIPPED, now);
                    taskInstanceRepository.save(instance);
                    outboxWriter.write("TASK_INSTANCE", instance.getId().toString(), "TASK_INSTANCE_SKIPPED",
                            Map.of("runId", runId.toString(), "taskName", taskName(instance.getDagTaskId()),
                                    "state", instance.getState().name()));
                    changed = true;
                }
            }
        } while (changed);

        finalizeRunIfComplete(runId);
    }

    /**
     * Finalizes the run if all task instances are terminal. Runs from the
     * worker's propagation path; concurrent propagators may race here, and an
     * optimistic-lock failure simply means another worker finalized it first.
     */
    @Transactional
    public void finalizeRunIfComplete(UUID runId) {
        DagRunEntity run = dagRunRepository.findById(runId).orElseThrow();
        if (RunStateMachine.isTerminal(run.getState())) {
            return;
        }
        List<TaskInstanceEntity> instances = taskInstanceRepository.findByRunIdOrderByQueuedAt(runId);
        if (instances.isEmpty()) {
            return;
        }
        boolean allTerminal = instances.stream().allMatch(t -> TaskStateMachine.isTerminal(t.getState()));
        if (!allTerminal) {
            return;
        }
        boolean anyFailed = instances.stream()
                .anyMatch(t -> t.getState() == TaskState.FAILED || t.getState() == TaskState.DEAD_LETTERED);
        boolean anyCancelled = instances.stream().anyMatch(t -> t.getState() == TaskState.CANCELLED);
        RunState finalState;
        if (anyCancelled) {
            finalState = RunState.CANCELLED;
        } else if (anyFailed) {
            finalState = RunState.FAILED;
        } else {
            finalState = RunState.SUCCESS;
        }
        Instant now = Instant.now();
        if (run.getState() == RunState.PENDING) {
            run.transition(RunState.RUNNING, now);
        }
        run.transition(finalState, now);
        dagRunRepository.save(run);
        outboxWriter.write("DAG_RUN", runId.toString(), "DAG_RUN_" + finalState.name(),
                Map.of("runId", runId.toString(), "dagId", run.getDagId().toString(), "state", finalState.name()));
        log.info("DAG run {} finalized as {}", runId, finalState);
    }

    @Transactional
    public DagRunEntity cancel(UUID dagId, UUID runId) {
        DagRunEntity run = dagRunRepository.findById(runId)
                .orElseThrow(() -> new DagService.NotFoundException("Run not found: " + runId));
        if (!run.getDagId().equals(dagId)) {
            throw new DagService.NotFoundException("Run not found: " + runId);
        }
        if (RunStateMachine.isTerminal(run.getState())) {
            return run;
        }
        Instant now = Instant.now();
        for (TaskInstanceEntity instance : taskInstanceRepository.findByRunIdOrderByQueuedAt(runId)) {
            if (!TaskStateMachine.isTerminal(instance.getState())) {
                instance.transition(TaskState.CANCELLED, now);
                taskInstanceRepository.save(instance);
            }
        }
        run.transition(RunState.CANCELLED, now);
        dagRunRepository.save(run);
        outboxWriter.write("DAG_RUN", runId.toString(), "DAG_RUN_CANCELLED",
                Map.of("runId", runId.toString(), "dagId", dagId.toString()));
        log.info("Cancelled DAG run {}", runId);
        return run;
    }

    @Transactional(readOnly = true)
    public DagRunEntity getRun(UUID runId) {
        return dagRunRepository.findById(runId)
                .orElseThrow(() -> new DagService.NotFoundException("Run not found: " + runId));
    }

    @Transactional(readOnly = true)
    public List<DagRunEntity> listRuns(UUID dagId, RunState state, int limit, int offset) {
        return dagRunRepository.findFiltered(dagId, state,
                org.springframework.data.domain.PageRequest.of(offset / Math.max(limit, 1), Math.max(limit, 1)));
    }

    @Transactional(readOnly = true)
    public List<TaskInstanceEntity> listTasks(UUID runId) {
        return taskInstanceRepository.findByRunIdOrderByQueuedAt(runId);
    }

    private String taskName(UUID dagTaskId) {
        return dagTaskRepository.findById(dagTaskId).map(DagTaskEntity::getName).orElse("unknown");
    }

    private String toJson(Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize trigger payload", e);
        }
    }
}
