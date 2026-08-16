package com.buildathon.orchestrator.service;

import com.buildathon.orchestrator.domain.TaskState;
import com.buildathon.orchestrator.outbox.OutboxWriter;
import com.buildathon.orchestrator.persistence.DeadLetterEntity;
import com.buildathon.orchestrator.persistence.DeadLetterRepository;
import com.buildathon.orchestrator.persistence.TaskInstanceEntity;
import com.buildathon.orchestrator.persistence.TaskInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dead-letter queue: listing and replay. Replaying resets the task to
 * SCHEDULED with the current time so a worker picks it up again.
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final DeadLetterRepository deadLetterRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final OutboxWriter outboxWriter;

    public DeadLetterService(DeadLetterRepository deadLetterRepository,
                             TaskInstanceRepository taskInstanceRepository,
                             OutboxWriter outboxWriter) {
        this.deadLetterRepository = deadLetterRepository;
        this.taskInstanceRepository = taskInstanceRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional(readOnly = true)
    public List<DeadLetterEntity> list(int limit) {
        return deadLetterRepository.findAllByOrderByDeadLetteredAtDesc(
                org.springframework.data.domain.PageRequest.of(0, Math.min(limit, 200)));
    }

    @Transactional
    public DeadLetterEntity replay(UUID id) {
        DeadLetterEntity dl = deadLetterRepository.findById(id)
                .orElseThrow(() -> new DagService.NotFoundException("Dead letter not found: " + id));
        if ("REPLAYED".equals(dl.getReplayStatus())) {
            return dl;
        }
        TaskInstanceEntity task = taskInstanceRepository.findById(dl.getTaskInstanceId())
                .orElseThrow(() -> new DagService.NotFoundException("Task instance not found: " + dl.getTaskInstanceId()));
        Instant now = Instant.now();
        task.transition(TaskState.SCHEDULED, now);
        task.scheduleRetry(task.getAttemptNo(), now);
        taskInstanceRepository.save(task);
        dl.setReplayStatus("REPLAYED");
        deadLetterRepository.save(dl);
        outboxWriter.write("DEAD_LETTER", id.toString(), "DEAD_LETTER_REPLAYED",
                Map.of("deadLetterId", id.toString(), "taskInstanceId", task.getId().toString(),
                        "runId", dl.getRunId().toString()));
        log.info("Replayed dead letter {} — task {} rescheduled", id, task.getId());
        return dl;
    }
}
