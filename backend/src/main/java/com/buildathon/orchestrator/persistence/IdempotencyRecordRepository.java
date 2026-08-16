package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;

public interface IdempotencyRecordRepository extends JpaRepository<IdempotencyRecordEntity, String> {

    void deleteByExpiresAtBefore(Instant now);
}
