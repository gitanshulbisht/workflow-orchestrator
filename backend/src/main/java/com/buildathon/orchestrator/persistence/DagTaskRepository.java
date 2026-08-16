package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DagTaskRepository extends JpaRepository<DagTaskEntity, UUID> {

    List<DagTaskEntity> findByDagIdOrderByName(UUID dagId);
}
