package com.buildathon.orchestrator.scheduling;

import com.buildathon.orchestrator.config.OrchestratorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Drives the scheduler loop: every scan interval, try to become leader and
 * run one cron scan plus one reaper pass while holding the lock.
 */
@Component
public class SchedulerRunner {

    private static final Logger log = LoggerFactory.getLogger(SchedulerRunner.class);

    private final OrchestratorProperties properties;
    private final LeaderElection leaderElection;
    private final CronScanner cronScanner;
    private final ReaperService reaperService;

    public SchedulerRunner(OrchestratorProperties properties, LeaderElection leaderElection,
                           CronScanner cronScanner, ReaperService reaperService) {
        this.properties = properties;
        this.leaderElection = leaderElection;
        this.cronScanner = cronScanner;
        this.reaperService = reaperService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!properties.hasRole("scheduler")) {
            log.info("Scheduler disabled (roles: {})", properties.roles());
        }
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduler.scan-interval-ms:2000}")
    public void tick() {
        if (!properties.hasRole("scheduler")) {
            return;
        }
        boolean leader = leaderElection.tryAcquire(0, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (!leader) {
            return;
        }
        try {
            cronScanner.scanOnce(Instant.now());
            reaperService.reapStaleTasks(Instant.now(), properties.worker().staleHeartbeatMs());
            reaperService.finalizeCompletedRuns();
        } catch (Exception e) {
            log.error("Scheduler tick failed", e);
        } finally {
            leaderElection.release();
        }
    }
}
