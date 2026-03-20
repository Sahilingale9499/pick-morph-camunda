package com.temporallearn.spring_temporal.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

@Entity
@Table(name = "ae_order")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AeOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "external_service_request_id", nullable = false, unique = true)
    private String externalServiceRequestId;

    @Column(name = "type", nullable = false)
    private String type;

    // JSON array string, e.g. ["assist_area"]
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "fulfillment_area", columnDefinition = "JSONB")
    private String fulfillmentArea;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "attributes", columnDefinition = "JSONB")
    private String attributes;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "expectations", columnDefinition = "JSONB")
    private String expectations;

    @Column(name = "state", nullable = false)
    private String state;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
