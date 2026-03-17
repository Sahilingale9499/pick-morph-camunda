package com.temporallearn.spring_temporal.dto;

import lombok.Data;

@Data
public class ValidationResult {
    private String orderId;              // Matches the PickId
    private String transactionId;        // Received from downstream
    private boolean success;             // True if validated
}
