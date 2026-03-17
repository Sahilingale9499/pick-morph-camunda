package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionStatusRepository extends JpaRepository<TransactionStatus, String> {
}
