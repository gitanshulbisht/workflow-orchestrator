package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagRunRepository;
import com.buildathon.orchestrator.persistence.DeadLetterRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dashboard stats. Cached in Redis for 10 seconds to absorb dashboard polling.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Stats", description = "Dashboard metrics")
public class StatsController {

    private static final String CACHE_KEY = "orchestrator:stats";
    private static final Duration CACHE_TTL = Duration.ofSeconds(10);

    private final DagRepository dagRepository;
    private final DagRunRepository dagRunRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final StringRedisTemplate redisTemplate;

    public StatsController(DagRepository dagRepository, DagRunRepository dagRunRepository,
                           TaskInstanceRepository taskInstanceRepository, DeadLetterRepository deadLetterRepository,
                           StringRedisTemplate redisTemplate) {
        this.dagRepository = dagRepository;
        this.dagRunRepository = dagRunRepository;
        this.taskInstanceRepository = taskInstanceRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.redisTemplate = redisTemplate;
    }

    @Operation(summary = "Aggregated dashboard stats (cached 10s)")
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        String cached = redisTemplate.opsForValue().get(CACHE_KEY);
        if (cached != null) {
            try {
                return new com.fasterxml.jackson.databind.ObjectMapper().readValue(cached, Map.class);
            } catch (Exception ignored) {
            }
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("dags", dagRepository.count());
        stats.put("runs", dagRunRepository.count());
        stats.put("tasks", taskInstanceRepository.count());
        stats.put("deadLetters", deadLetterRepository.count());
        try {
            redisTemplate.opsForValue().set(CACHE_KEY,
                    new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(stats), CACHE_TTL);
        } catch (Exception ignored) {
        }
        return stats;
    }
}
