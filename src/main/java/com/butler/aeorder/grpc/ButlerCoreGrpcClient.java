package com.butler.aeorder.grpc;

import com.greyorange.butler.core.grpc.ButlerCoreServiceGrpc;
import com.greyorange.butler.core.grpc.CarrierInfo;
import com.greyorange.butler.core.grpc.ExceptionData;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusRequest;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;
import com.greyorange.butler.core.grpc.NodePickDetails;
import com.greyorange.butler.core.grpc.NodePickDetailsInfo;
import com.greyorange.butler.core.grpc.PickedItemInfo;
import com.greyorange.butler.core.grpc.PpsBinId;
import com.greyorange.butler.core.grpc.UpdatePickInstructionRequest;
import com.greyorange.butler.core.grpc.UpdatePickInstructionResponse;
import com.butler.aeorder.dto.UpdatePickInstructionDto;
import com.butler.aeorder.dto.UpdatePickInstructionResult;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ButlerCoreGrpcClient {

    @Value("${butler.core.grpc.address:gmc_butler_server:9090}")
    private String address;

    @Value("${butler.core.grpc.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${butler.core.grpc.mock-enabled:false}")
    private boolean mockEnabled;

    private ManagedChannel channel;
    private ButlerCoreServiceGrpc.ButlerCoreServiceBlockingStub blockingStub;

    @PostConstruct
    public void init() {
        log.info("Initializing Butler Core gRPC client - address: {}, mockEnabled: {}", address, mockEnabled);

        if (!mockEnabled) {
            try {
                String[] parts = address.split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9090;

                // Resolve hostname to IP to bypass gRPC's strict DNS name validation.
                // Docker hostnames with underscores (e.g. gmc_butler_server) are invalid
                // per DNS RFC but Docker's internal DNS resolves them fine via Java's
                // InetAddress. We pass the resolved IP to gRPC instead.
                InetAddress resolved = InetAddress.getByName(host);
                String resolvedIp = resolved.getHostAddress();
                log.info("Resolved Butler Core host '{}' to IP '{}'", host, resolvedIp);

                channel = ManagedChannelBuilder.forAddress(resolvedIp, port)
                        .usePlaintext() // Use TLS in production
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Butler Core gRPC channel for address: " + address, e);
            }

            blockingStub = ButlerCoreServiceGrpc.newBlockingStub(channel);
            log.info("Butler Core gRPC client initialized successfully");
        } else {
            log.warn("Butler Core gRPC client running in MOCK mode - will return simulated responses");
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Butler Core gRPC client");
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while shutting down gRPC channel", e);
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Update pick instruction with full transaction details in Butler Core.
     * Returns a classified result with retriable/non-retriable error info
     * instead of throwing, so the workflow can decide how to handle errors.
     *
     * @param dto the UpdatePickInstructionDto containing all pick update fields
     * @return UpdatePickInstructionResult with status, error code, and retriable flag
     */
    public UpdatePickInstructionResult updatePickInstruction(UpdatePickInstructionDto dto) {
        log.info("Calling Butler Core to update pick instruction - orderId: {}, transactionId: {}, ppsId: {}",
                dto.getOrderId(), dto.getTransactionId(), dto.getPpsId());

        // Return mock response if mock mode is enabled
        if (mockEnabled) {
            log.info("MOCK MODE: Returning simulated response for pick instruction update");
            return UpdatePickInstructionResult.ok("SUCCESS",
                    "Mock response - pick instruction updated successfully");
        }

        UpdatePickInstructionRequest request = buildUpdateRequest(dto);

        try {
            UpdatePickInstructionResponse response = blockingStub
                    .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .updatePickInstruction(request);

            log.info("Butler Core response - status: {}, message: {}",
                    response.getStatus(), response.getMessage());

            // Check response-level error status
            String status = response.getStatus();
            if ("FAILURE".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status)) {
                // Response-level failures are treated as non-retriable (server processed
                // the request but rejected it due to business/validation logic)
                log.warn("Butler Core returned failure response - status: {}, message: {}",
                        status, response.getMessage());
                return UpdatePickInstructionResult.permanentError(
                        "RESPONSE_" + status.toUpperCase(),
                        response.getMessage());
            }

            return UpdatePickInstructionResult.ok(status, response.getMessage());

        } catch (StatusRuntimeException e) {
            String grpcCode = e.getStatus().getCode().name();
            String description = e.getStatus().getDescription();
            log.error("gRPC call to Butler Core failed - code: {}, description: {}",
                    grpcCode, description, e);

            // Classify gRPC error as retriable or non-retriable
            if (UpdatePickInstructionResult.isRetriableGrpcCode(grpcCode)) {
                log.warn("RETRIABLE gRPC error [{}] — workflow should retry. description: {}",
                        grpcCode, description);
                return UpdatePickInstructionResult.retriableError(grpcCode,
                        "gRPC " + grpcCode + ": " + description);
            } else {
                log.error("NON-RETRIABLE gRPC error [{}] — workflow should fail permanently. description: {}",
                        grpcCode, description);
                return UpdatePickInstructionResult.permanentError(grpcCode,
                        "gRPC " + grpcCode + ": " + description);
            }
        } catch (Exception e) {
            // Unexpected exceptions are treated as retriable (could be transient infra issues)
            log.error("Unexpected error calling Butler Core - treating as retriable", e);
            return UpdatePickInstructionResult.retriableError(
                    "UNEXPECTED",
                    "Unexpected error: " + e.getMessage());
        }
    }

    /**
     * Get pick instruction status from Butler Core.
     *
     * @param pickId the pick instruction ID
     * @return GetPickInstructionStatusResponse with current status
     */
    public GetPickInstructionStatusResponse getPickInstructionStatus(String pickId) {
        log.info("Getting pick instruction status from Butler Core - pickId: {}", pickId);

        // Return mock response if mock mode is enabled
        if (mockEnabled) {
            log.info("MOCK MODE: Returning simulated status for pick instruction");
            return GetPickInstructionStatusResponse.newBuilder()
                    .setPickId(pickId)
                    .setStatus("IN_PROGRESS")
                    .setIsComplete(false)
                    .setTotalQty(10)
                    .setProcessedQty(0)
                    .setRemainingQty(10)
                    .build();
        }

        GetPickInstructionStatusRequest request = GetPickInstructionStatusRequest.newBuilder()
                .setPickId(pickId)
                .build();

        try {
            GetPickInstructionStatusResponse response = blockingStub
                    .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .getPickInstructionStatus(request);

            log.info("Butler Core status response - pickId: {}, status: {}, isComplete: {}",
                    response.getPickId(), response.getStatus(), response.getIsComplete());

            return response;

        } catch (StatusRuntimeException e) {
            log.error("gRPC call to get pick instruction status failed - status: {}, description: {}",
                    e.getStatus().getCode(), e.getStatus().getDescription(), e);
            throw new RuntimeException("Failed to get pick instruction status from Butler Core: " + e.getMessage(), e);
        }
    }

    // ---- Private helper methods to build proto messages from DTOs ----

    private UpdatePickInstructionRequest buildUpdateRequest(UpdatePickInstructionDto dto) {
        UpdatePickInstructionRequest.Builder builder = UpdatePickInstructionRequest.newBuilder();

        if (dto.getPickedItemInfoList() != null) {
            builder.addAllPickedItemInfoList(
                dto.getPickedItemInfoList().stream()
                    .map(this::toPickedItemInfo)
                    .collect(Collectors.toList())
            );
        }
        if (dto.getSlotref() != null) builder.setSlotref(dto.getSlotref());
        if (dto.getOrderId() != null) builder.setOrderId(dto.getOrderId());
        if (dto.getNodePickedDetails() != null) {
            builder.addAllNodePickedDetails(
                dto.getNodePickedDetails().stream()
                    .map(this::toNodePickDetails)
                    .collect(Collectors.toList())
            );
        }
        if (dto.getPpsbinId() != null) {
            builder.setPpsbinId(PpsBinId.newBuilder()
                    .setPpsId(dto.getPpsbinId().getPpsId())
                    .setBinId(dto.getPpsbinId().getBinId() != null ? dto.getPpsbinId().getBinId() : "")
                    .build());
        }
        builder.setPpsId(dto.getPpsId());
        if (dto.getPpsbinList() != null) builder.addAllPpsbinList(dto.getPpsbinList());
        if (dto.getPpsPoint() != null) builder.setPpsPoint(dto.getPpsPoint());
        builder.setIsMarkedContainerScanned(dto.isMarkedContainerScanned());
        if (dto.getSeatName() != null) builder.setSeatName(dto.getSeatName());
        if (dto.getTransactionId() != null) builder.setTransactionId(dto.getTransactionId());
        if (dto.getChecklists() != null) builder.addAllChecklists(dto.getChecklists());
        if (dto.getCarrierInfo() != null) {
            UpdatePickInstructionDto.CarrierInfoDto ci = dto.getCarrierInfo();
            builder.setCarrierInfo(CarrierInfo.newBuilder()
                    .setType(ci.getType() != null ? ci.getType() : "")
                    .setCarrierType(ci.getCarrierType() != null ? ci.getCarrierType() : "")
                    .setBarcode(ci.getBarcode() != null ? ci.getBarcode() : "")
                    .build());
        }
        if (dto.getIrtBinSerial() != null) builder.setIrtBinSerial(dto.getIrtBinSerial());
        if (dto.getUserLoggedIn() != null) builder.setUserLoggedIn(dto.getUserLoggedIn());
        if (dto.getDestBarcode() != null) builder.setDestBarcode(dto.getDestBarcode());
        if (dto.getDestRollcageBarcode() != null) builder.setDestRollcageBarcode(dto.getDestRollcageBarcode());
        if (dto.getBarcodeDataScanned() != null) builder.setBarcodeDataScanned(dto.getBarcodeDataScanned());
        if (dto.getDanglingArea() != null) builder.setDanglingArea(dto.getDanglingArea());

        return builder.build();
    }

    private PickedItemInfo toPickedItemInfo(UpdatePickInstructionDto.PickedItemInfoDto dto) {
        PickedItemInfo.Builder builder = PickedItemInfo.newBuilder()
                .setTpid(dto.getTpid())
                .setUom(dto.getUom() != null ? dto.getUom() : "")
                .setItemUid(dto.getItemUid() != null ? dto.getItemUid() : "")
                .setPickedQty(dto.getPickedQty());

        if (dto.getPickInstructionIds() != null) {
            builder.addAllPickInstructionIds(dto.getPickInstructionIds());
        }

        if (dto.getException() != null) {
            UpdatePickInstructionDto.ExceptionDataDto ex = dto.getException();
            builder.setException(ExceptionData.newBuilder()
                    .setMissing(ex.getMissing())
                    .setUnscannable(ex.getUnscannable())
                    .setPhysicallyDamaged(ex.getPhysicallyDamaged())
                    .setChecklistException(ex.getChecklistException())
                    .build());
        }

        return builder.build();
    }

    private NodePickDetails toNodePickDetails(UpdatePickInstructionDto.NodePickDetailsDto dto) {
        NodePickDetails.Builder builder = NodePickDetails.newBuilder()
                .setId(dto.getId() != null ? dto.getId() : "")
                .setType(dto.getType() != null ? dto.getType() : "");

        if (dto.getChildren() != null) {
            builder.addAllChildren(
                dto.getChildren().stream()
                    .map(this::toNodePickDetails)
                    .collect(Collectors.toList())
            );
        }

        if (dto.getDetails() != null) {
            UpdatePickInstructionDto.NodePickDetailsInfoDto info = dto.getDetails();
            NodePickDetailsInfo.Builder infoBuilder = NodePickDetailsInfo.newBuilder()
                    .setQuantity(info.getQuantity())
                    .setTransactionType(info.getTransactionType() != null ? info.getTransactionType() : "");
            if (info.getBarcodeReferences() != null) {
                infoBuilder.addAllBarcodeReferences(info.getBarcodeReferences());
            }
            builder.setDetails(infoBuilder.build());
        }

        return builder.build();
    }
}
