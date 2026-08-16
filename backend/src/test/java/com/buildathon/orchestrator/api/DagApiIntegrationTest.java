package com.buildathon.orchestrator.api;

import com.buildathon.orchestrator.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class DagApiIntegrationTest extends AbstractIntegrationTest {

    private static final String VALID_YAML = """
            name: test-dag
            description: A test DAG
            tasks:
              - name: first
                type: delay
                config:
                  seconds: 1
              - name: second
                type: delay
                config:
                  seconds: 1
                dependsOn: [first]
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private com.buildathon.orchestrator.persistence.DagRepository dagRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.DagTaskRepository dagTaskRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.TaskDependencyRepository taskDependencyRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private com.buildathon.orchestrator.persistence.DagRunRepository dagRunRepository;

    @AfterEach
    void tearDown() {
        taskInstanceRepository.deleteAll();
        dagRunRepository.deleteAll();
        taskDependencyRepository.deleteAll();
        dagTaskRepository.deleteAll();
        dagRepository.deleteAll();
    }

    @Test
    void registersDagAndReturns201() throws Exception {
        String response = mockMvc.perform(post("/api/v1/dags")
                        .contentType("application/yaml")
                        .content(VALID_YAML))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("test-dag"))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.tasks").isArray())
                .andExpect(jsonPath("$.tasks.length()").value(2))
                .andReturn().getResponse().getContentAsString();

        assertThat(response).contains("\"id\"");
        assertThat(dagRepository.findByName("test-dag")).isPresent();
        assertThat(dagTaskRepository.findByDagIdOrderByName(dagRepository.findByName("test-dag").get().getId()))
                .hasSize(2);
    }

    @Test
    void rejectsInvalidDagWithProblemDetails() throws Exception {
        mockMvc.perform(post("/api/v1/dags")
                        .contentType("application/yaml")
                        .content("""
                                name: bad-dag
                                tasks:
                                  - name: a
                                    type: delay
                                    dependsOn: [a]
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").exists())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("cycle")));
    }

    @Test
    void rejectsDuplicateDagName() throws Exception {
        mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isConflict());
    }

    @Test
    void idempotencyKeyReturnsSameResponseWithoutDuplicate() throws Exception {
        String first = mockMvc.perform(post("/api/v1/dags")
                        .header("Idempotency-Key", "key-123")
                        .contentType("application/yaml")
                        .content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String second = mockMvc.perform(post("/api/v1/dags")
                        .header("Idempotency-Key", "key-123")
                        .contentType("application/yaml")
                        .content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(mapper.readTree(first)).isEqualTo(mapper.readTree(second));
        assertThat(dagRepository.count()).isEqualTo(1);
    }

    @Test
    void conflictingIdempotencyKeyWithDifferentBodyIsRejected() throws Exception {
        mockMvc.perform(post("/api/v1/dags")
                        .header("Idempotency-Key", "key-conflict")
                        .contentType("application/yaml")
                        .content(VALID_YAML))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/dags")
                        .header("Idempotency-Key", "key-conflict")
                        .contentType("application/yaml")
                        .content(VALID_YAML.replace("test-dag", "other-dag")))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    void listReturnsRegisteredDags() throws Exception {
        mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/dags"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].name").value("test-dag"));
    }

    @Test
    void getReturnsDagWithTasksAndDependencies() throws Exception {
        String created = mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String id = extractId(created);

        mockMvc.perform(get("/api/v1/dags/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("test-dag"))
                .andExpect(jsonPath("$.tasks.length()").value(2));
    }

    @Test
    void patchPausesAndResumesDag() throws Exception {
        String created = mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String id = extractId(created);

        mockMvc.perform(patch("/api/v1/dags/{id}", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"paused\": true}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(true));

        assertThat(dagRepository.findById(java.util.UUID.fromString(id)).orElseThrow().isPaused()).isTrue();
    }

    @Test
    void patchWithNewYamlBumpsVersion() throws Exception {
        String created = mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String id = extractId(created);

        mockMvc.perform(patch("/api/v1/dags/{id}", id)
                        .contentType("application/yaml")
                        .content(VALID_YAML.replace("A test DAG", "Updated description")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(2));
    }

    @Test
    void patchWithNewYamlSucceedsWhenHistoricalTaskInstancesExist() throws Exception {
        String created = mockMvc.perform(post("/api/v1/dags").contentType("application/yaml").content(VALID_YAML))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String id = extractId(created);

        // Trigger a run so task_instance rows reference the current dag_task rows.
        mockMvc.perform(post("/api/v1/dags/{id}/runs", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated());

        // Updating the definition must not try to delete referenced dag_task rows.
        mockMvc.perform(patch("/api/v1/dags/{id}", id)
                        .contentType("application/yaml")
                        .content(VALID_YAML.replace("A test DAG", "Updated again")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(2));

        // The DAG listing resolves only the current version's tasks.
        mockMvc.perform(get("/api/v1/dags/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks.length()").value(2));
    }

    private String extractId(String json) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(json).get("id").asText();
    }
}
