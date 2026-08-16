package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    List<OutboxEventEntity> findByDeliveryStatusOrderByIdAsc(String deliveryStatus, org.springframework.data.domain.Pageable pageable);
}
