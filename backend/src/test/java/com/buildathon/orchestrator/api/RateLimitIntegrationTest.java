package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class RateLimitIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private com.buildathon.orchestrator.persistence.DagRepository dagRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.DagTaskRepository dagTaskRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.TaskDependencyRepository taskDependencyRepository;

    @DynamicPropertySource
    static void rateLimitProps(DynamicPropertyRegistry registry) {
        registry.add("orchestrator.rate-limit.enabled", () -> "true");
        registry.add("orchestrator.rate-limit.permits-per-second", () -> "2");
        registry.add("orchestrator.rate-limit.burst", () -> "2");
    }

    @BeforeEach
    void setUp() throws Exception {
        taskDependencyRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
        mockMvc.perform(post("/api/v1/dags")
                        .contentType("application/yaml")
                        .content("""
                                name: rate-dag
                                tasks:
                                  - name: t
                                    type: delay
                                    config:
                                      seconds: 0
                                """))
                .andExpect(status().isCreated());
    }

    @Test
    void burstsBeyondLimitAreRejectedWith429() throws Exception {
        // Use a dedicated API key so this test is isolated from other traffic.
        mockMvc.perform(get("/api/v1/dags").header("X-API-Key", "burst-key"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/dags").header("X-API-Key", "burst-key"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/dags").header("X-API-Key", "burst-key"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));
    }

    @Test
    void statsEndpointReturnsMetrics() throws Exception {
        mockMvc.perform(get("/api/v1/stats").header("X-API-Key", "stats-key"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.dags").isNumber())
                .andExpect(jsonPath("$.runs").isNumber())
                .andExpect(jsonPath("$.tasks").isNumber());
    }
}
