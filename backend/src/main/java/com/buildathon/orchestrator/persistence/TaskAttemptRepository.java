package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskAttemptRepository extends JpaRepository<TaskAttemptEntity, UUID> {

    List<TaskAttemptEntity> findByTaskInstanceIdOrderByAttemptNo(UUID taskInstanceId);

    java.util.Optional<TaskAttemptEntity> findByTaskInstanceIdAndAttemptNo(UUID taskInstanceId, int attemptNo);
}
