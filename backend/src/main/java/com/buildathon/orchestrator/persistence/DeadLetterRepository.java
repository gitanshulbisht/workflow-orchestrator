package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeadLetterRepository extends JpaRepository<DeadLetterEntity, UUID> {

    List<DeadLetterEntity> findAllByOrderByDeadLetteredAtDesc(org.springframework.data.domain.Pageable pageable);

    Optional<DeadLetterEntity> findByTaskInstanceId(UUID taskInstanceId);
}
