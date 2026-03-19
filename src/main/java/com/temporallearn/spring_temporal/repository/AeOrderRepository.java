package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.AeOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AeOrderRepository extends JpaRepository<AeOrder, Long> {
    Optional<AeOrder> findByExternalServiceRequestId(String externalServiceRequestId);
}
