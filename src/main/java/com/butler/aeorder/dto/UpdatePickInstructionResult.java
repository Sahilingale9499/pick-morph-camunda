package com.butler.aeorder.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of an UpdatePickInstruction call to Butler Core.
 * Carries the response status along with error classification
 * so the workflow can decide whether to retry or fail permanently.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdatePickInstructionResult {

    private String status;       // e.g. "SUCCESS", "FAILURE", "ERROR"
    private String message;      // Human-readable message from Butler Core
    private String errorCode;    // gRPC status code or response-level error code
    private boolean success;     // true if the call succeeded
    private boolean retriable;   // true if the error is transient and can be retried

    /**
     * gRPC status codes classified as retriable (transient/infrastructure errors).
     * UNAVAILABLE      - server not reachable, connection refused, load balancer errors
     * DEADLINE_EXCEEDED - timeout
     * RESOURCE_EXHAUSTED - rate limiting, out of memory
     * ABORTED          - concurrency conflict, retry safe
     * INTERNAL         - unexpected server-side error (may be transient)
     * UNKNOWN          - unclassified server error (may be transient)
     */
    private static final java.util.Set<String> RETRIABLE_GRPC_CODES = java.util.Set.of(
            "UNAVAILABLE",
            "DEADLINE_EXCEEDED",
            "RESOURCE_EXHAUSTED",
            "ABORTED",
            "INTERNAL",
            "UNKNOWN"
    );

    /**
     * gRPC status codes classified as non-retriable (permanent/logic errors).
     * INVALID_ARGUMENT    - bad request data
     * NOT_FOUND           - resource doesn't exist
     * ALREADY_EXISTS      - duplicate
     * PERMISSION_DENIED   - authorization failure
     * UNAUTHENTICATED     - auth missing/invalid
     * FAILED_PRECONDITION - state precondition not met
     * UNIMPLEMENTED       - RPC not supported
     * OUT_OF_RANGE        - value out of bounds
     * DATA_LOSS           - unrecoverable data loss
     */
    private static final java.util.Set<String> NON_RETRIABLE_GRPC_CODES = java.util.Set.of(
            "INVALID_ARGUMENT",
            "NOT_FOUND",
            "ALREADY_EXISTS",
            "PERMISSION_DENIED",
            "UNAUTHENTICATED",
            "FAILED_PRECONDITION",
            "UNIMPLEMENTED",
            "OUT_OF_RANGE",
            "DATA_LOSS"
    );

    public static boolean isRetriableGrpcCode(String grpcStatusCode) {
        return RETRIABLE_GRPC_CODES.contains(grpcStatusCode);
    }

    public static boolean isNonRetriableGrpcCode(String grpcStatusCode) {
        return NON_RETRIABLE_GRPC_CODES.contains(grpcStatusCode);
    }

    /** Factory: successful response */
    public static UpdatePickInstructionResult ok(String status, String message) {
        return UpdatePickInstructionResult.builder()
                .status(status)
                .message(message)
                .success(true)
                .retriable(false)
                .build();
    }

    /** Factory: retriable error */
    public static UpdatePickInstructionResult retriableError(String errorCode, String message) {
        return UpdatePickInstructionResult.builder()
                .status("ERROR")
                .message(message)
                .errorCode(errorCode)
                .success(false)
                .retriable(true)
                .build();
    }

    /** Factory: non-retriable (permanent) error */
    public static UpdatePickInstructionResult permanentError(String errorCode, String message) {
        return UpdatePickInstructionResult.builder()
                .status("ERROR")
                .message(message)
                .errorCode(errorCode)
                .success(false)
                .retriable(false)
                .build();
    }
}
