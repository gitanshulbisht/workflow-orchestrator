package com.buildathon.orchestrator.scheduling;

import com.buildathon.orchestrator.persistence.DagRepository;
import com.buildathon.orchestrator.persistence.DagScheduleEntity;
import com.buildathon.orchestrator.persistence.DagScheduleRepository;
import com.buildathon.orchestrator.service.RunService;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scans dag_schedule rows for due runs and triggers them. Misfire policy:
 * SKIP — trigger one run and advance to the next future fire time;
 * BACKFILL — trigger one run per missed fire time.
 */
@Component
public class CronScanner {

    private static final Logger log = LoggerFactory.getLogger(CronScanner.class);

    private final DagScheduleRepository scheduleRepository;
    private final DagRepository dagRepository;
    private final RunService runService;
    private final CronParser cronParser;

    public CronScanner(DagScheduleRepository scheduleRepository, DagRepository dagRepository,
                       RunService runService, CronParser cronParser) {
        this.scheduleRepository = scheduleRepository;
        this.dagRepository = dagRepository;
        this.runService = runService;
        this.cronParser = cronParser;
    }

    public int scanOnce(Instant now) {
        List<DagScheduleEntity> due = scheduleRepository.findDue(now, PageRequest.of(0, 50));
        int triggered = 0;
        for (DagScheduleEntity schedule : due) {
            var dag = dagRepository.findById(schedule.getDagId());
            if (dag.isEmpty() || dag.get().isPaused()) {
                continue;
            }
            if (schedule.getMisfirePolicy().equalsIgnoreCase("BACKFILL")) {
                triggered += backfill(schedule, dag.get().getScheduleCron(), dag.get().getTimezone(), now);
            } else {
                triggered += triggerOnce(schedule, now);
            }
        }
        if (triggered > 0) {
            log.info("Cron scan triggered {} runs", triggered);
        }
        return triggered;
    }

    @Transactional
    protected int triggerOnce(DagScheduleEntity schedule, Instant now) {
        try {
            runService.trigger(schedule.getDagId(), "SCHEDULED", Map.of("scheduledFor", now.toString()), null);
        } catch (Exception e) {
            log.error("Failed to trigger scheduled run for DAG {}", schedule.getDagId(), e);
            return 0;
        }
        schedule.setLastRunAt(now);
        schedule.setNextRunAt(nextFire(schedule.getDagId(), now));
        scheduleRepository.save(schedule);
        return 1;
    }

    @Transactional
    protected int backfill(DagScheduleEntity schedule, String cronExpr, String timezone, Instant now) {
        int count = 0;
        Instant next = nextFire(schedule.getDagId(), schedule.getLastRunAt() == null
                ? schedule.getNextRunAt() : schedule.getLastRunAt());
        while (next != null && next.isBefore(now)) {
            try {
                runService.trigger(schedule.getDagId(), "SCHEDULED", Map.of("scheduledFor", next.toString()), null);
                count++;
            } catch (Exception e) {
                log.error("Failed to backfill run for DAG {}", schedule.getDagId(), e);
                break;
            }
            next = nextFire(schedule.getDagId(), next);
        }
        schedule.setLastRunAt(now);
        schedule.setNextRunAt(next);
        scheduleRepository.save(schedule);
        return count;
    }

    private Instant nextFire(java.util.UUID dagId, Instant after) {
        Optional<DagScheduleEntity> schedule = scheduleRepository.findByDagId(dagId);
        String cronExpr = dagRepository.findById(dagId).map(d -> d.getScheduleCron()).orElse(null);
        if (cronExpr == null || cronExpr.isBlank()) {
            return null;
        }
        try {
            Cron cron = cronParser.parse(cronExpr);
            cron.validate();
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            Optional<ZonedDateTime> next = executionTime.nextExecution(
                    ZonedDateTime.ofInstant(after, java.time.ZoneId.of("UTC")));
            return next.map(zdt -> zdt.toInstant()).orElse(null);
        } catch (Exception e) {
            log.error("Cannot compute next fire time for DAG {} with cron {}", dagId, cronExpr, e);
            return null;
        }
    }
}
