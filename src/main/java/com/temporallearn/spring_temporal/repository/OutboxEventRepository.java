package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.OutboxEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Fetches all unpublished entries with a PESSIMISTIC_WRITE lock.
     * Used by the background poller — prevents concurrent publish by onOutboxEvent.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT e FROM OutboxEvent e WHERE e.published = false ORDER BY e.id ASC")
    List<OutboxEvent> findUnpublishedForUpdate();

    /**
     * Fetches a single outbox entry by ID with a PESSIMISTIC_WRITE lock.
     * Used by onOutboxEvent — prevents concurrent publish by the poller.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT e FROM OutboxEvent e WHERE e.id = :id")
    Optional<OutboxEvent> findByIdForUpdate(Long id);
}
