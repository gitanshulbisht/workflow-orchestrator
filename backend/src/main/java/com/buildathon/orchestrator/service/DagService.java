package com.buildathon.orchestrator.service;

import com.buildathon.orchestrator.domain.DagSpec;
import com.buildathon.orchestrator.domain.DagValidator;
import com.buildathon.orchestrator.domain.TaskSpec;
import com.buildathon.orchestrator.persistence.DagEntity;
import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagScheduleEntity;
import com.buildathon.orchestrator.persistence.DagScheduleRepository;
import com.buildathon.orchestrator.persistence.DagTaskEntity;
import com.buildathon.orchestrator.persistence.DagTaskRepository;
import com.buildathon.orchestrator.persistence.TaskDependencyEntity;
import com.buildathon.orchestrator.persistence.TaskDependencyRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class DagService {

    private static final Logger log = LoggerFactory.getLogger(DagService.class);

    private static final String LIST_CACHE_KEY = "orchestrator:dags:list";
    private static final String DAG_CACHE_PREFIX = "orchestrator:dags:";
    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    private final DagRepository dagRepository;
    private final DagTaskRepository dagTaskRepository;
    private final TaskDependencyRepository taskDependencyRepository;
    private final DagScheduleRepository dagScheduleRepository;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final com.cronutils.parser.CronParser cronParser;

    public DagService(DagRepository dagRepository, DagTaskRepository dagTaskRepository,
                      TaskDependencyRepository taskDependencyRepository, DagScheduleRepository dagScheduleRepository,
                      ObjectMapper objectMapper, StringRedisTemplate redisTemplate,
                      com.cronutils.parser.CronParser cronParser) {
        this.dagRepository = dagRepository;
        this.dagTaskRepository = dagTaskRepository;
        this.taskDependencyRepository = taskDependencyRepository;
        this.dagScheduleRepository = dagScheduleRepository;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.cronParser = cronParser;
    }

    private Instant computeNextFire(String cronExpr, Instant after) {
        try {
            var cron = cronParser.parse(cronExpr);
            cron.validate();
            var executionTime = com.cronutils.model.time.ExecutionTime.forCron(cron);
            return executionTime.nextExecution(
                            java.time.ZonedDateTime.ofInstant(after, java.time.ZoneId.of("UTC")))
                    .map(java.time.ZonedDateTime::toInstant)
                    .orElse(null);
        } catch (Exception e) {
            log.warn("Cannot compute next fire time for cron {}: {}", cronExpr, e.getMessage());
            return null;
        }
    }

    @Transactional
    public DagEntity register(String yaml) {
        DagSpec spec = com.buildathon.orchestrator.domain.DagParser.parse(yaml);
        List<String> errors = DagValidator.validate(spec);
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
        if (dagRepository.existsByName(spec.name())) {
            throw new ConflictException("DAG with name '" + spec.name() + "' already exists");
        }

        Instant now = Instant.now();
        DagEntity dag = new DagEntity(UUID.randomUUID(), spec.name(), spec.description(), 1,
                spec.scheduleCron(), spec.timezone() == null ? "UTC" : spec.timezone(),
                false, yaml, now, now);
        dagRepository.save(dag);

        if (spec.scheduleCron() != null && !spec.scheduleCron().isBlank()) {
            Instant next = computeNextFire(spec.scheduleCron(), now);
            dagScheduleRepository.save(new DagScheduleEntity(dag.getId(), next, null, "SKIP"));
        }

        Map<String, UUID> taskIds = new HashMap<>();
        for (TaskSpec task : spec.tasks()) {
            UUID taskId = UUID.randomUUID();
            taskIds.put(task.name(), taskId);
            dagTaskRepository.save(new DagTaskEntity(taskId, dag.getId(), task.name(), task.type(),
                    toJson(task.config()), task.maxRetries(), task.retryDelaySeconds(),
                    task.retryBackoff(), task.timeoutSeconds(), task.singleton()));
        }
        for (TaskSpec task : spec.tasks()) {
            for (String dep : task.dependsOn()) {
                taskDependencyRepository.save(new TaskDependencyEntity(taskIds.get(task.name()), taskIds.get(dep)));
            }
        }
        log.info("Registered DAG '{}' (id={})", dag.getName(), dag.getId());
        invalidateCache();
        return dag;
    }

    @Transactional(readOnly = true)
    public List<DagEntity> list() {
        return dagRepository.findAll();
    }

    @Transactional(readOnly = true)
    public DagEntity get(UUID id) {
        return dagRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("DAG not found: " + id));
    }

    @Transactional(readOnly = true)
    public List<DagTaskEntity> getTasks(UUID dagId) {
        return dagTaskRepository.findByDagIdOrderByName(dagId);
    }

    @Transactional(readOnly = true)
    public Map<UUID, List<UUID>> getDependencies(UUID dagId) {
        Map<UUID, List<UUID>> result = new HashMap<>();
        List<DagTaskEntity> tasks = dagTaskRepository.findByDagIdOrderByName(dagId);
        for (DagTaskEntity task : tasks) {
            List<UUID> deps = taskDependencyRepository.findByIdTaskId(task.getId())
                    .stream().map(TaskDependencyEntity::getDependsOnTaskId).toList();
            result.put(task.getId(), deps);
        }
        return result;
    }

    @Transactional
    public DagEntity pause(UUID id, boolean paused) {
        DagEntity dag = get(id);
        dag.setPaused(paused, Instant.now());
        dagRepository.save(dag);
        invalidateCache();
        return dag;
    }

    @Transactional
    public DagEntity updateDefinition(UUID id, String yaml) {
        DagEntity existing = get(id);
        DagSpec spec = com.buildathon.orchestrator.domain.DagParser.parse(yaml);
        List<String> errors = DagValidator.validate(spec);
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
        if (!spec.name().equals(existing.getName())) {
            throw new ValidationException(List.of("cannot change DAG name; create a new DAG instead"));
        }
        dagRepository.findByName(spec.name())
                .filter(other -> !other.getId().equals(id))
                .ifPresent(other -> {
                    throw new ConflictException("DAG with name '" + spec.name() + "' already exists");
                });

        // Replace task graph: delete old tasks and dependencies (cascade), insert new.
        List<DagTaskEntity> oldTasks = dagTaskRepository.findByDagIdOrderByName(id);
        oldTasks.forEach(task -> taskDependencyRepository.deleteAll(taskDependencyRepository.findByIdTaskId(task.getId())));
        taskDependencyRepository.flush();
        dagTaskRepository.deleteAll(oldTasks);
        dagTaskRepository.flush();

        existing.update(spec.description(), existing.getVersion() + 1, spec.scheduleCron(),
                spec.timezone() == null ? existing.getTimezone() : spec.timezone(),
                existing.isPaused(), yaml, Instant.now());
        dagRepository.save(existing);

        Map<String, UUID> taskIds = new HashMap<>();
        for (TaskSpec task : spec.tasks()) {
            UUID taskId = UUID.randomUUID();
            taskIds.put(task.name(), taskId);
            dagTaskRepository.save(new DagTaskEntity(taskId, existing.getId(), task.name(), task.type(),
                    toJson(task.config()), task.maxRetries(), task.retryDelaySeconds(),
                    task.retryBackoff(), task.timeoutSeconds(), task.singleton()));
        }
        for (TaskSpec task : spec.tasks()) {
            for (String dep : task.dependsOn()) {
                taskDependencyRepository.save(new TaskDependencyEntity(taskIds.get(task.name()), taskIds.get(dep)));
            }
        }
        log.info("Updated DAG '{}' to version {}", existing.getName(), existing.getVersion());
        invalidateCache();
        return existing;
    }

    private void invalidateCache() {
        try {
            redisTemplate.delete(LIST_CACHE_KEY);
            // The per-DAG cache is namespaced by key prefix; the list cache is the
            // dashboard-facing one. Individual DAG reads use short TTL entries that
            // are cheap to recompute; on update we clear those too.
            redisTemplate.delete(redisTemplate.keys(DAG_CACHE_PREFIX + "*"));
        } catch (Exception e) {
            log.warn("Cache invalidation failed (ignoring): {}", e.getMessage());
        }
    }

    private String toJson(Map<String, Object> config) {
        try {
            return objectMapper.writeValueAsString(config);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize task config", e);
        }
    }

    public static class ValidationException extends RuntimeException {
        private final List<String> errors;

        public ValidationException(List<String> errors) {
            super("Validation failed: " + String.join("; ", errors));
            this.errors = errors;
        }

        public List<String> getErrors() {
            return errors;
        }
    }

    public static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}
