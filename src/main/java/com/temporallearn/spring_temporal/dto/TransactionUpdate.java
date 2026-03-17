package com.temporallearn.spring_temporal.dto;

import lombok.Data;

@Data
public class TransactionUpdate {
    private String pickId;
    private String transactionId;
    private String command;          // e.g., "UPDATE", "CANCEL", "COMPLETE", "RETRY"
    private String transactionType;  // e.g., "PICK", "DROP", "SCAN"
    private int processedQty;
    private String status;           // e.g., "IN_PROGRESS", "COMPLETED", "FAILED"
    private String timestamp;
    private String additionalInfo;
}
