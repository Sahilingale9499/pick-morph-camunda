package com.butler.aeorder.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.AePickListRequest;
import com.butler.aeorder.model.AeOrder;
import com.butler.aeorder.model.AeOrdersMapping;
import com.butler.aeorder.model.TransactionStatus;
import com.butler.aeorder.repository.AeOrderRepository;
import com.butler.aeorder.repository.AeOrdersMappingRepository;
import com.butler.aeorder.repository.TransactionStatusRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.butler.aeorder.model.AeOrdersMapping;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Handles all PostgreSQL persistence for AE orders and transaction statuses.
 */
@Service
@Slf4j
public class AeOrderPersistenceService {

    private final AeOrderRepository aeOrderRepository;
    private final AeOrdersMappingRepository aeOrdersMappingRepository;
    private final TransactionStatusRepository transactionStatusRepository;
    private final ObjectMapper objectMapper;

    public AeOrderPersistenceService(AeOrderRepository aeOrderRepository,
                                     AeOrdersMappingRepository aeOrdersMappingRepository,
                                     TransactionStatusRepository transactionStatusRepository,
                                     ObjectMapper objectMapper) {
        this.aeOrderRepository = aeOrderRepository;
        this.aeOrdersMappingRepository = aeOrdersMappingRepository;
        this.transactionStatusRepository = transactionStatusRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Persists the parent PICK order and each PICK_LINE child, plus the mapping rows.
     * Idempotent: skips if an AeOrder with the same externalServiceRequestId already exists.
     * Transactional: all-or-nothing — partial writes are rolled back on failure.
     *
     * @throws RuntimeException if serialization or DB write fails
     */
    @Transactional
    public void saveAeOrder(AePickListRequest request) {
        // Idempotency: skip if already persisted (Kafka redelivery, race condition)
        Optional<AeOrder> existing = aeOrderRepository
                .findByExternalServiceRequestId(request.getExternalServiceRequestId());
        if (existing.isPresent()) {
            log.warn("AeOrder already exists for externalServiceRequestId: {} — skipping",
                    request.getExternalServiceRequestId());
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        try {
            // 1. Save parent
            AeOrder parent = AeOrder.builder()
                    .externalServiceRequestId(request.getExternalServiceRequestId())
                    .type(request.getType())
                    .fulfillmentArea(objectMapper.writeValueAsString(request.getFulfillmentArea()))
                    .attributes(objectMapper.writeValueAsString(request.getAttributes()))
                    .expectations(null)
                    .state("CREATED")
                    .subState("CREATED")
                    .status("CREATED")
                    .actuals("{}")
                    .createdAt(now)
                    .updatedAt(now)
                    .build();
            aeOrderRepository.save(parent);

            // 2. Save each child + mapping
            for (AePickListRequest.AeServiceRequestLine line : request.getServiceRequests()) {
                AeOrder child = AeOrder.builder()
                        .externalServiceRequestId(line.getExternalServiceRequestId())
                        .type(line.getType())
                        .fulfillmentArea(objectMapper.writeValueAsString(request.getFulfillmentArea()))
                        .attributes(objectMapper.writeValueAsString(line.getAttributes()))
                        .expectations(objectMapper.writeValueAsString(line.getExpectations()))
                        .state("CREATED")
                        .subState("CREATED")
                        .status("CREATED")
                        .actuals("{}")
                        .createdAt(now)
                        .updatedAt(now)
                        .build();
                aeOrderRepository.save(child);

                AeOrdersMapping mapping = AeOrdersMapping.builder()
                        .parentExternalServiceRequestId(request.getExternalServiceRequestId())
                        .childExternalServiceRequestId(line.getExternalServiceRequestId())
                        .build();
                aeOrdersMappingRepository.save(mapping);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist AeOrder for externalServiceRequestId: "
                    + request.getExternalServiceRequestId(), e);
        }
    }

    /**
     * Persists or updates the transaction status for the given txId.
     * No-op if txId is null or empty.
     * Exceptions propagate to the caller so constraint violations are visible.
     */
    public void saveTransactionStatus(String txId, String pickId, String status) {
        if (txId == null || txId.isEmpty()) {
            return;
        }
        TransactionStatus ts = new TransactionStatus(txId, pickId, status, Instant.now());
        transactionStatusRepository.save(ts);
    }

    /**
     * Looks up a persisted transaction status by txId.
     */
    public Optional<TransactionStatus> findTransactionStatus(String txId) {
        return transactionStatusRepository.findById(txId);
    }

    /**
     * Marks the parent AE order and all its PICK_LINE children as FAILED.
     * Sets state, status, and sub_state to "FAILED" on each record.
     * Called on the BPMN validation-failure path instead of deleting the record.
     */
    @Transactional
    public void markAsFailed(String pickId) {
        LocalDateTime now = LocalDateTime.now();

        aeOrderRepository.findByExternalServiceRequestId(pickId).ifPresent(parent -> {
            parent.setState("FAILED");
            parent.setStatus("FAILED");
            parent.setSubState("FAILED");
            parent.setUpdatedAt(now);
            aeOrderRepository.save(parent);
            log.info("Marked parent AE order as FAILED | externalServiceRequestId: {}", pickId);
        });

        List<AeOrdersMapping> mappings = aeOrdersMappingRepository
                .findByParentExternalServiceRequestId(pickId);
        for (AeOrdersMapping mapping : mappings) {
            aeOrderRepository.findByExternalServiceRequestId(
                    mapping.getChildExternalServiceRequestId()).ifPresent(child -> {
                child.setState("FAILED");
                child.setStatus("FAILED");
                child.setSubState("FAILED");
                child.setUpdatedAt(now);
                aeOrderRepository.save(child);
                log.info("Marked child AE order as FAILED | externalServiceRequestId: {}",
                        mapping.getChildExternalServiceRequestId());
            });
        }
    }
}
