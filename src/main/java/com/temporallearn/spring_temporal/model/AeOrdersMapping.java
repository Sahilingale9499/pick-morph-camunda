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

@Entity
@Table(name = "ae_orders_mapping")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AeOrdersMapping {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "parent_external_service_request_id", nullable = false)
    private String parentExternalServiceRequestId;

    @Column(name = "child_external_service_request_id", nullable = false)
    private String childExternalServiceRequestId;
}
