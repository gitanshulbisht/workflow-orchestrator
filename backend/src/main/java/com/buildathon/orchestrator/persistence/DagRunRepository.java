package com.buildathon.orchestrator.persistence;

import com.buildathon.orchestrator.domain.RunState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DagRunRepository extends JpaRepository<DagRunEntity, UUID> {

    Optional<DagRunEntity> findByIdempotencyKey(String idempotencyKey);

    @Query("""
            select r from DagRunEntity r
            where (:dagId is null or r.dagId = :dagId)
              and (:state is null or r.state = :state)
            order by r.createdAt desc
            """)
    List<DagRunEntity> findFiltered(@Param("dagId") UUID dagId, @Param("state") RunState state,
                                    org.springframework.data.domain.Pageable pageable);

    long countByDagIdAndStateIn(UUID dagId, java.util.Collection<RunState> states);

    @Query("select r from DagRunEntity r where r.state = :state and r.startedAt < :staleBefore")
    List<DagRunEntity> findStaleRunning(@Param("state") RunState state, @Param("staleBefore") java.time.Instant staleBefore);
}
