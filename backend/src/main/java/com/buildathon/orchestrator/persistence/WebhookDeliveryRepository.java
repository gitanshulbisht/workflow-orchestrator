package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface WebhookDeliveryRepository extends JpaRepository<WebhookDeliveryEntity, UUID> {

    List<WebhookDeliveryEntity> findByStatusOrderByEventIdAsc(String status, org.springframework.data.domain.Pageable pageable);

    boolean existsByWebhookIdAndEventId(UUID webhookId, Long eventId);
}
