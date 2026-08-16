package com.buildathon.orchestrator.persistence;

import com.buildathon.orchestrator.domain.TaskState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskInstanceRepository extends JpaRepository<TaskInstanceEntity, UUID> {

    List<TaskInstanceEntity> findByRunIdOrderByQueuedAt(UUID runId);

    @Query("""
            select t from TaskInstanceEntity t
            where t.runId = :runId
              and t.state = com.buildathon.orchestrator.domain.TaskState.PENDING
              and not exists (
                  select 1 from TaskDependencyEntity d
                  where d.id.taskId = t.dagTaskId
                    and d.id.dependsOnTaskId in (
                        select u.dagTaskId from TaskInstanceEntity u
                        where u.runId = :runId and u.state <> com.buildathon.orchestrator.domain.TaskState.SUCCESS
                    )
              )
            """)
    List<TaskInstanceEntity> findReadyInRun(@Param("runId") UUID runId);

    /**
     * The heart of the work queue: claim the next due SCHEDULED tasks with a row-level
     * lock, skipping rows locked by other workers (Postgres SKIP LOCKED). Guarantees
     * at-most-once claiming across a distributed worker pool. The native query is
     * deliberate: JPA pagination would trigger follow-on locking with stale re-reads.
     */
    @Query(value = """
            select * from task_instance t
            where t.state = 'SCHEDULED' and t.scheduled_at <= :now
            order by t.scheduled_at asc
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskInstanceEntity> claimNextDue(@Param("now") Instant now, @Param("limit") int limit);

    List<TaskInstanceEntity> findByRunIdAndState(UUID runId, TaskState state);

    long countByRunIdAndStateNot(UUID runId, TaskState state);

    long countByRunIdAndState(UUID runId, TaskState state);

    @Query("select t from TaskInstanceEntity t where t.state = com.buildathon.orchestrator.domain.TaskState.RUNNING and t.heartbeatAt < :staleBefore")
    List<TaskInstanceEntity> findStaleRunning(@Param("staleBefore") Instant staleBefore);

    @Query("select t from TaskInstanceEntity t where t.state = com.buildathon.orchestrator.domain.TaskState.UP_FOR_RETRY and t.scheduledAt <= :now")
    List<TaskInstanceEntity> findDueRetries(@Param("now") Instant now);
}
