package com.butler.aeorder.dto;

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
    @JsonDeserialize(using = FlexibleStringDeserializer.class)
    private String serviceRequestId;         // order_id — integer on SUCCESS, null on FAILURE
    private String executionId;
    private String status;                   // "SUCCESS" or "FAILURE"
    private String errorCode;                // e.g. "VALIDATION_FAILED" — present on FAILURE
    private String message;
    private List<ServiceRequest> serviceRequests; // present on SUCCESS
    private List<ErrorDetail> errors;             // present on FAILURE
    private String timestamp;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ServiceRequest {
        private String externalServiceRequestId;
        @JsonDeserialize(using = FlexibleStringDeserializer.class)
        private String serviceRequestId;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDetail {
        private String field;
        private String code;
        private String message;
    }

    /**
     * Accepts both JSON strings and integers, converting both to String.
     * Needed because SRMS sends serviceRequestId as an integer (e.g. 123456) on SUCCESS
     * and null on FAILURE.
     */
    public static class FlexibleStringDeserializer extends StdDeserializer<String> {
        public FlexibleStringDeserializer() { super(String.class); }

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            JsonToken token = p.currentToken();
            if (token == JsonToken.VALUE_STRING || token == JsonToken.VALUE_NUMBER_INT) {
                return p.getValueAsString();
            }
            throw ctx.wrongTokenException(p, String.class, JsonToken.VALUE_STRING,
                    "serviceRequestId must be a JSON string or integer, not " + token);
        }
    }
}
