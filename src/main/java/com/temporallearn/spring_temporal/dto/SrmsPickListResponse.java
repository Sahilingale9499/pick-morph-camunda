package com.temporallearn.spring_temporal.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SrmsPickListResponse {

    private String externalServiceRequestId; // pick_instruction_id = pickId — workflow routing key
    private Integer serviceRequestId;        // order_id — null on FAILURE
    private String executionId;              // null on FAILURE
    private String status;                   // "SUCCESS" or "FAILURE"
    private String errorCode;               // e.g. "VALIDATION_FAILED" — present on FAILURE
    private String message;
    private List<ServiceRequest> serviceRequests;
    private List<ErrorDetail> errors;
    private String timestamp;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ServiceRequest {
        private String externalServiceRequestId; // pick instruction sub-item ID
        private Integer serviceRequestId;        // order line ID
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDetail {
        private String field;
        private String code;
        private String message;
    }
}
