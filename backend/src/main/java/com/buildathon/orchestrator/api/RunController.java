package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.domain.RunState;
import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.persistence.DagRunEntity;
import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.buildathon.orchestrator.service.RunService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Runs", description = "Trigger and inspect DAG runs")
public class RunController {

    private final RunService runService;

    public RunController(RunService runService) {
        this.runService = runService;
    }

    @Operation(summary = "Trigger a DAG run (manual)")
    @PostMapping("/dags/{dagId}/runs")
    public ResponseEntity<Map<String, Object>> trigger(
            @PathVariable UUID dagId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) Map<String, Object> payload) {
        DagRunEntity run = runService.trigger(dagId, "MANUAL", payload, idempotencyKey);
        return ResponseEntity.created(URI.create("/api/v1/runs/" + run.getId()))
                .body(toRunMap(run));
    }

    @Operation(summary = "Cancel a running DAG run")
    @PostMapping("/dags/{dagId}/runs/{runId}/cancel")
    public Map<String, Object> cancel(@PathVariable UUID dagId, @PathVariable UUID runId) {
        return toRunMap(runService.cancel(dagId, runId));
    }

    @Operation(summary = "List runs with optional filters")
    @GetMapping("/runs")
    public List<Map<String, Object>> list(
            @RequestParam(required = false) UUID dagId,
            @RequestParam(required = false) RunState state,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return runService.listRuns(dagId, state, Math.min(limit, 200), offset)
                .stream().map(this::toRunMap).toList();
    }

    @Operation(summary = "Get a run with its task timeline")
    @GetMapping("/runs/{runId}")
    public Map<String, Object> get(@PathVariable UUID runId) {
        DagRunEntity run = runService.getRun(runId);
        Map<String, Object> result = toRunMap(run);
        result.put("tasks", runService.listTasks(runId).stream().map(this::toTaskMap).toList());
        return result;
    }

    @Operation(summary = "List task instances of a run")
    @GetMapping("/runs/{runId}/tasks")
    public List<Map<String, Object>> tasks(@PathVariable UUID runId) {
        return runService.listTasks(runId).stream().map(this::toTaskMap).toList();
    }

    private Map<String, Object> toRunMap(DagRunEntity run) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", run.getId());
        map.put("dagId", run.getDagId());
        map.put("dagVersion", run.getDagVersion());
        map.put("state", run.getState());
        map.put("triggerType", run.getTriggerType());
        map.put("triggerPayload", run.getTriggerPayload());
        map.put("startedAt", run.getStartedAt());
        map.put("endedAt", run.getEndedAt());
        map.put("createdAt", run.getCreatedAt());
        return map;
    }

    private Map<String, Object> toTaskMap(TaskInstanceEntity task) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", task.getId());
        map.put("runId", task.getRunId());
        map.put("dagTaskId", task.getDagTaskId());
        map.put("state", task.getState());
        map.put("attemptNo", task.getAttemptNo());
        map.put("queuedAt", task.getQueuedAt());
        map.put("scheduledAt", task.getScheduledAt());
        map.put("startedAt", task.getStartedAt());
        map.put("endedAt", task.getEndedAt());
        map.put("claimedBy", task.getClaimedBy());
        map.put("errorMessage", task.getErrorMessage());
        return map;
    }
}
