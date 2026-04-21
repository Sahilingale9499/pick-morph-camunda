package greymatter.butler.aeorder.dto.ae;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Envelope DTO for messages consumed from the <tenant>.pick-list.response Kafka topic.
 * Published by SRMS after processing a pick-list request from AE.
 *
 * Example:
 * {
 *   "source_service": "srms",
 *   "timestamp": "2026-03-24T13:00:00Z",
 *   "message_id": "d84cef01-...",
 *   "entity_id": "Kellanova_order999977132",
 *   "name": "pick_list_response",
 *   "payload": { ...Payload fields... },
 *   "context": { "execution_id": "0" }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PickListResponseEvent {

    @JsonProperty("source_service")
    private String sourceService;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("entity_id")
    private String entityId;

    @JsonProperty("name")
    private String name;

    @JsonProperty("payload")
    private Payload payload;

    @JsonProperty("context")
    private Context context;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {

        @JsonProperty("externalServiceRequestId")
        private String externalServiceRequestId;

        /** AE-internal order ID. Null on FAILURE. */
        @JsonProperty("serviceRequestId")
        private Long serviceRequestId;

        /** AE-internal execution ID. Null on FAILURE. */
        @JsonProperty("executionId")
        private String executionId;

        /** "SUCCESS" or "FAILURE" */
        @JsonProperty("status")
        private String status;

        /** Error code on FAILURE (e.g. VALIDATION_FAILED). Null on SUCCESS. */
        @JsonProperty("errorCode")
        private String errorCode;

        @JsonProperty("message")
        private String message;

        /** Populated only on FAILURE with per-field validation errors. */
        @JsonProperty("errors")
        private List<FieldError> errors;

        @JsonProperty("timestamp")
        private String timestamp;

        /** Per-orderline service request IDs returned on SUCCESS. */
        @JsonProperty("serviceRequests")
        private List<ServiceRequestRef> serviceRequests;

        public boolean isSuccess() {
            return "SUCCESS".equalsIgnoreCase(status);
        }

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class FieldError {

            @JsonProperty("field")
            private String field;

            @JsonProperty("code")
            private String code;

            @JsonProperty("message")
            private String message;
        }

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ServiceRequestRef {

            @JsonProperty("externalServiceRequestId")
            private String externalServiceRequestId;

            @JsonProperty("serviceRequestId")
            private Long serviceRequestId;
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Context {

        @JsonProperty("execution_id")
        private String executionId;
    }
}
