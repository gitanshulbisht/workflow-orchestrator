package com.buildathon.orchestrator.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.List;
import java.util.Map;

/**
 * Parses DAG definitions from YAML (primary) or JSON. Uses Jackson with a YAML factory.
 */
public final class DagParser {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private DagParser() {
    }

    public static DagSpec parse(String source) {
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("DAG definition is empty");
        }
        try {
            ObjectMapper mapper = source.trim().startsWith("{") ? JSON : YAML;
            return mapper.readValue(source, DagSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse DAG definition: " + e.getMessage(), e);
        }
    }

    public static String toJson(DagSpec spec) {
        try {
            return JSON.writeValueAsString(spec);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize DAG spec", e);
        }
    }

    public static DagSpec fromJson(String json) {
        try {
            return JSON.readValue(json, DagSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse DAG JSON: " + e.getMessage(), e);
        }
    }
}
