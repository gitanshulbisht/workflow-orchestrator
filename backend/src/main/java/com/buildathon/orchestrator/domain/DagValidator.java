package com.buildathon.orchestrator.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Pure-domain validation for DAG specifications. No Spring, fully unit-testable.
 */
public final class DagValidator {

    public static final int MAX_RETRIES = 10;
    public static final int MAX_RETRY_DELAY_SECONDS = 3600;
    public static final int MAX_TIMEOUT_SECONDS = 86400;
    public static final int MAX_TASKS = 100;

    private DagValidator() {
    }

    public static List<String> validate(DagSpec spec) {
        List<String> errors = new ArrayList<>();
        if (spec.name() == null || spec.name().isBlank()) {
            errors.add("name is required");
        } else if (!spec.name().matches("[a-zA-Z0-9_.-]{1,100}")) {
            errors.add("name must match [a-zA-Z0-9_.-] and be at most 100 chars");
        }
        if (spec.tasks() == null || spec.tasks().isEmpty()) {
            errors.add("at least one task is required");
            return errors;
        }
        if (spec.tasks().size() > MAX_TASKS) {
            errors.add("too many tasks (max " + MAX_TASKS + ")");
        }
        if (spec.scheduleCron() != null && !spec.scheduleCron().isBlank()) {
            try {
                com.cronutils.model.Cron cron = new com.cronutils.parser.CronParser(
                        com.cronutils.model.definition.CronDefinitionBuilder
                                .instanceDefinitionFor(com.cronutils.model.CronType.QUARTZ))
                        .parse(spec.scheduleCron());
                cron.validate();
            } catch (IllegalArgumentException e) {
                errors.add("invalid cron expression: " + e.getMessage());
            }
        }

        Set<String> names = new HashSet<>();
        for (TaskSpec task : spec.tasks()) {
            if (task.name() == null || task.name().isBlank()) {
                errors.add("task name is required");
                continue;
            }
            if (!names.add(task.name())) {
                errors.add("duplicate task name: " + task.name());
            }
            if (task.type() == null || !Set.of("bash", "http", "delay", "fail").contains(task.type())) {
                errors.add("task '" + task.name() + "': type must be one of bash, http, delay, fail");
            }
            if (task.maxRetries() < 0 || task.maxRetries() > MAX_RETRIES) {
                errors.add("task '" + task.name() + "': maxRetries must be between 0 and " + MAX_RETRIES);
            }
            if (task.retryDelaySeconds() < 1 || task.retryDelaySeconds() > MAX_RETRY_DELAY_SECONDS) {
                errors.add("task '" + task.name() + "': retryDelaySeconds must be between 1 and " + MAX_RETRY_DELAY_SECONDS);
            }
            if (task.retryBackoff() < 1.0 || task.retryBackoff() > 10.0) {
                errors.add("task '" + task.name() + "': retryBackoff must be between 1.0 and 10.0");
            }
            if (task.timeoutSeconds() < 1 || task.timeoutSeconds() > MAX_TIMEOUT_SECONDS) {
                errors.add("task '" + task.name() + "': timeoutSeconds must be between 1 and " + MAX_TIMEOUT_SECONDS);
            }
            if ("bash".equals(task.type()) && !(task.config().get("command") instanceof String cmd && !cmd.isBlank())) {
                errors.add("task '" + task.name() + "': bash tasks require a 'command' config");
            }
            if ("http".equals(task.type()) && !(task.config().get("url") instanceof String url && url.startsWith("http"))) {
                errors.add("task '" + task.name() + "': http tasks require a valid 'url' config");
            }
        }

        Map<String, TaskSpec> byName = new HashMap<>();
        for (TaskSpec task : spec.tasks()) {
            byName.put(task.name(), task);
        }
        for (TaskSpec task : spec.tasks()) {
            for (String dep : task.dependsOn()) {
                if (!byName.containsKey(dep)) {
                    errors.add("task '" + task.name() + "' depends on unknown task '" + dep + "'");
                }
            }
        }

        List<String> cycle = findCycle(byName);
        if (cycle != null) {
            errors.add("cycle detected: " + String.join(" -> ", cycle));
        }
        return errors;
    }

    /**
     * Kahn's algorithm. Returns the first cycle found (as an ordered list) or null.
     */
    public static List<String> findCycle(Map<String, TaskSpec> tasksByName) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        for (String name : tasksByName.keySet()) {
            inDegree.put(name, 0);
            dependents.put(name, new ArrayList<>());
        }
        for (TaskSpec task : tasksByName.values()) {
            for (String dep : task.dependsOn()) {
                if (!tasksByName.containsKey(dep)) {
                    continue;
                }
                inDegree.merge(task.name(), 1, Integer::sum);
                dependents.get(dep).add(task.name());
            }
        }
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> e : inDegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        Set<String> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            String name = queue.poll();
            visited.add(name);
            for (String dependent : dependents.get(name)) {
                if (inDegree.merge(dependent, -1, Integer::sum) == 0) {
                    queue.add(dependent);
                }
            }
        }
        if (visited.size() == tasksByName.size()) {
            return null;
        }
        // Reconstruct one cycle for a helpful error message.
        Map<String, String> parent = new HashMap<>();
        for (String name : tasksByName.keySet()) {
            if (visited.contains(name)) {
                continue;
            }
            // DFS from an unvisited node; every unvisited node is part of a cycle.
            String found = dfsFindCycle(name, parent, new HashSet<>(), new HashSet<>(), dependents);
            if (found != null) {
                List<String> cycle = new ArrayList<>();
                cycle.add(found);
                String cur = parent.get(found);
                while (cur != null && !cur.equals(found)) {
                    cycle.add(0, cur);
                    cur = parent.get(cur);
                }
                cycle.add(0, found);
                return cycle;
            }
        }
        return List.of("unknown");
    }

    private static String dfsFindCycle(String node, Map<String, String> parent, Set<String> visiting,
                                       Set<String> done, Map<String, List<String>> dependents) {
        visiting.add(node);
        for (String next : dependents.getOrDefault(node, List.of())) {
            if (done.contains(next)) {
                continue;
            }
            parent.put(next, node);
            if (visiting.contains(next)) {
                return next;
            }
            String found = dfsFindCycle(next, parent, visiting, done, dependents);
            if (found != null) {
                return found;
            }
        }
        visiting.remove(node);
        done.add(node);
        return null;
    }
}
