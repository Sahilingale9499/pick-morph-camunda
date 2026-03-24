package com.butler.aeorder.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import com.greyorange.butler.pick.order.grpc.PickOrderServiceGrpc;
import com.greyorange.butler.pick.order.grpc.PickOrderServiceProto.GetOrderDataRequest;
import com.greyorange.butler.pick.order.grpc.PickOrderServiceProto.GetOrderDataResponse;

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

@Component
@Slf4j
public class ButlerPickOrderGrpcClient {

    @Value("${butler.pick.grpc.address:gmc_butler_server:50051}")
    private String address;

    @Value("${butler.pick.grpc.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${butler.pick.grpc.mock-enabled:false}")
    private boolean mockEnabled;

    private final ObjectMapper objectMapper;

    private ManagedChannel channel;
    private PickOrderServiceGrpc.PickOrderServiceBlockingStub blockingStub;

    public ButlerPickOrderGrpcClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        log.info("Initializing Butler Pick Order gRPC client - address: {}, mockEnabled: {}", address, mockEnabled);

        if (!mockEnabled) {
            try {
                String[] parts = address.split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50051;

                // Resolve hostname to IP to bypass gRPC's strict DNS name validation.
                // Docker hostnames with underscores (e.g. gmc_butler_server) are invalid
                // per DNS RFC but Docker's internal DNS resolves them fine via Java's
                // InetAddress. We pass the resolved IP to gRPC instead.
                InetAddress resolved = InetAddress.getByName(host);
                String resolvedIp = resolved.getHostAddress();
                log.info("Resolved Butler Pick Order host '{}' to IP '{}'", host, resolvedIp);

                channel = ManagedChannelBuilder.forAddress(resolvedIp, port)
                        .usePlaintext()
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .build();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to initialize Butler Pick Order gRPC channel for address: " + address, e);
            }

            blockingStub = PickOrderServiceGrpc.newBlockingStub(channel);
            log.info("Butler Pick Order gRPC client initialized successfully");
        } else {
            log.warn("Butler Pick Order gRPC client running in MOCK mode - will return null responses");
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Butler Pick Order gRPC client");
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
     * Fetches the full order node data for the given orderId from butler_server via gRPC.
     *
     * Returns the order data as a JsonNode (structurally identical to the former HTTP response),
     * so callers can navigate it with the same field-path accessors as before.
     *
     * Returns null on any failure — callers are responsible for treating null as fatal.
     */
    public JsonNode getOrderData(String orderId) {
        log.info("Fetching order data via gRPC - orderId: {}", orderId);

        if (mockEnabled) {
            log.warn("MOCK MODE: Butler Pick Order gRPC client returning null for orderId: {}", orderId);
            return null;
        }

        GetOrderDataRequest request = GetOrderDataRequest.newBuilder()
                .setOrderId(orderId)
                .build();

        try {
            GetOrderDataResponse response = blockingStub
                    .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .getOrderData(request);

            // google.protobuf.Struct is a well-known type: JsonFormat serializes it as a
            // plain JSON object (not wrapped in {"fields": ...}), so the result is
            // structurally identical to what the HTTP /api/v2/order_node endpoint returned.
            String json = JsonFormat.printer().print(response.getOrderData());
            JsonNode node = objectMapper.readTree(json);
            log.debug("Received order data for orderId: {}", orderId);
            return node;

        } catch (StatusRuntimeException e) {
            log.error("gRPC call to get order data failed - orderId: {}, code: {}, description: {}",
                    orderId, e.getStatus().getCode(), e.getStatus().getDescription(), e);
            return null;
        } catch (Exception e) {
            log.error("Unexpected error fetching order data via gRPC - orderId: {}", orderId, e);
            return null;
        }
    }
}
