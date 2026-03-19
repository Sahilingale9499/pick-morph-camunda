package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.AeOrdersMapping;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AeOrdersMappingRepository extends JpaRepository<AeOrdersMapping, Long> {
    List<AeOrdersMapping> findByParentExternalServiceRequestId(String parentExternalServiceRequestId);
}
