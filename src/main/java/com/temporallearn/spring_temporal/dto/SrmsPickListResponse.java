package com.temporallearn.spring_temporal.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.Data;
import java.io.IOException;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SrmsPickListResponse {

    private String externalServiceRequestId; // pick_instruction_id = pickId — workflow routing key
    @JsonDeserialize(using = StrictStringDeserializer.class)
    private String serviceRequestId;         // order_id — null on FAILURE
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
        @JsonDeserialize(using = StrictStringDeserializer.class)
        private String serviceRequestId;         // order line ID
    }

    public static class StrictStringDeserializer extends StdDeserializer<String> {
        public StrictStringDeserializer() { super(String.class); }

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            if (p.currentToken() != JsonToken.VALUE_STRING) {
                throw ctx.wrongTokenException(p, String.class, JsonToken.VALUE_STRING,
                        "serviceRequestId must be a JSON string, not " + p.currentToken());
            }
            return p.getText();
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDetail {
        private String field;
        private String code;
        private String message;
    }
}
