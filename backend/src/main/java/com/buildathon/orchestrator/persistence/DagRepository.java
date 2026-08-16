package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface DagRepository extends JpaRepository<DagEntity, UUID> {

    Optional<DagEntity> findByName(String name);

    boolean existsByName(String name);
}
