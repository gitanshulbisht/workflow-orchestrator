package com.buildathon.orchestrator.worker;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Worker role loop: every tick, submits claim+execute passes to a bounded
 * pool. WorkerService claims tasks with SKIP LOCKED inside its own short
 * transactions, so concurrent workers can never grab the same task.
 */
@Component
public class WorkerLoop {

    private static final Logger log = LoggerFactory.getLogger(WorkerLoop.class);

    private final OrchestratorProperties properties;
    private final WorkerService workerService;
    private final String workerId = UUID.randomUUID().toString().substring(0, 8);
    private final ExecutorService pool;

    public WorkerLoop(OrchestratorProperties properties, WorkerService workerService) {
        this.properties = properties;
        this.workerService = workerService;
        this.pool = Executors.newFixedThreadPool(Math.max(1, properties.worker().concurrency()));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!properties.hasRole("worker")) {
            log.info("Worker disabled (roles: {})", properties.roles());
        } else {
            log.info("Worker {} started with concurrency {}", workerId, properties.worker().concurrency());
        }
    }

    @Scheduled(fixedDelayString = "${orchestrator.worker.poll-interval-ms:500}")
    public void poll() {
        if (!properties.hasRole("worker")) {
            return;
        }
        try {
            workerService.scheduleDueRetries();
        } catch (Exception e) {
            log.error("Retry re-arm failed on worker {}", workerId, e);
        }
        for (int i = 0; i < properties.worker().concurrency(); i++) {
            pool.submit(this::executeSafely);
        }
    }

    private void executeSafely() {
        try {
            workerService.executeNext(workerId);
        } catch (Exception e) {
            log.error("Task execution failed on worker {}", workerId, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        pool.shutdown();
        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
