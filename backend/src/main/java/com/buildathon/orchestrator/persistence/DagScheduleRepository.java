package com.buildathon.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface DagScheduleRepository extends JpaRepository<DagScheduleEntity, UUID> {

    Optional<DagScheduleEntity> findByDagId(UUID dagId);

    @Query("select s from DagScheduleEntity s where s.nextRunAt is not null and s.nextRunAt <= :now order by s.nextRunAt asc")
    java.util.List<DagScheduleEntity> findDue(@Param("now") java.time.Instant now, org.springframework.data.domain.Pageable pageable);
}
