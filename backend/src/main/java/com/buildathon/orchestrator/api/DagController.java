package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.api.dto.DagResponse;
import com.buildathon.orchestrator.persistence.DagEntity;
import com.buildathon.orchestrator.persistence.DagTaskEntity;
import com.buildathon.orchestrator.service.DagService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dags")
@Tag(name = "DAGs", description = "Register and manage DAG definitions")
public class DagController {

    private final DagService dagService;
    private final ObjectMapper objectMapper;

    public DagController(DagService dagService, ObjectMapper objectMapper) {
        this.dagService = dagService;
        this.objectMapper = objectMapper;
    }

    @Operation(summary = "Register a new DAG (YAML or JSON body)",
            responses = @ApiResponse(responseCode = "201", description = "DAG registered",
                    headers = @Header(name = "Location", description = "URL of the created DAG")))
    @PostMapping(consumes = {MediaType.APPLICATION_JSON_VALUE, "application/yaml", "text/yaml", "application/x-yaml"})
    public ResponseEntity<DagResponse> register(
            @Parameter(description = "DAG definition in YAML or JSON") @RequestBody String body) {
        DagEntity dag = dagService.register(body);
        return ResponseEntity.created(URI.create("/api/v1/dags/" + dag.getId()))
                .body(toResponse(dag));
    }

    @Operation(summary = "List all DAGs")
    @GetMapping
    public List<DagResponse> list() {
        return dagService.list().stream().map(this::toResponse).toList();
    }

    @Operation(summary = "Get a DAG with its task graph")
    @GetMapping("/{id}")
    public DagResponse get(@PathVariable UUID id) {
        return toResponse(dagService.get(id));
    }

    @Operation(summary = "Pause/resume a DAG")
    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public DagResponse patch(@PathVariable UUID id, @RequestBody Map<String, Object> patch) {
        DagEntity dag = dagService.get(id);
        if (patch.containsKey("paused")) {
            boolean paused = (Boolean) patch.get("paused");
            dag = dagService.pause(id, paused);
        }
        return toResponse(dag);
    }

    @Operation(summary = "Update a DAG definition (bumps version)")
    @PatchMapping(value = "/{id}", consumes = {"application/yaml", "text/yaml", "application/x-yaml"})
    public DagResponse updateYaml(@PathVariable UUID id, @RequestBody String yaml) {
        return toResponse(dagService.updateDefinition(id, yaml));
    }

    private DagResponse toResponse(DagEntity dag) {
        List<DagTaskEntity> tasks = dagService.getTasks(dag.getId());
        Map<UUID, List<UUID>> deps = dagService.getDependencies(dag.getId());
        List<DagResponse.TaskResponse> taskResponses = tasks.stream()
                .map(task -> new DagResponse.TaskResponse(
                        task.getId(),
                        task.getName(),
                        task.getTaskType(),
                        parseConfig(task.getConfig()),
                        task.getMaxRetries(),
                        task.getRetryDelaySeconds(),
                        task.getRetryBackoff(),
                        task.getTimeoutSeconds(),
                        task.isSingleton(),
                        deps.getOrDefault(task.getId(), List.of())))
                .toList();
        return new DagResponse(
                dag.getId(),
                dag.getName(),
                dag.getDescription(),
                dag.getVersion(),
                dag.getScheduleCron(),
                dag.getTimezone(),
                dag.isPaused(),
                dag.getDagYaml(),
                taskResponses);
    }

    private Map<String, Object> parseConfig(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            return Map.of();
        }
    }
}
