package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskDependencyRepository extends JpaRepository<TaskDependencyEntity, TaskDependencyEntity.TaskDependencyId> {

    List<TaskDependencyEntity> findByIdTaskId(UUID taskId);
}
