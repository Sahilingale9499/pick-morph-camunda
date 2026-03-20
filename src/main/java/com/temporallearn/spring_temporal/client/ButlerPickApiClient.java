package com.temporallearn.spring_temporal.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP client for butler_server's pick API.
 *
 * Fetches raw order_node data used to enrich AePickListRequest with
 * product attributes (filter_parameters, tag_parameters, package_parameters)
 * and order options (bintags, simple_priority, behaviour) that are not
 * available in the pick-instruction Kafka message.
 */
@Component
@Slf4j
public class ButlerPickApiClient {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String baseUrl;

    public ButlerPickApiClient(RestTemplate restTemplate,
                               ObjectMapper objectMapper,
                               @Value("${butler.http.host}") String host,
                               @Value("${butler.http.port}") int port) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.baseUrl = "http://" + host + ":" + port;
    }

    /**
     * Fetches the raw order_node record from butler_server for the given orderId.
     *
     * Returns the full response as a JsonNode, or null if the call fails for any reason.
     * Callers must treat null as a fatal error (no AE order should be created).
     */
    public JsonNode getOrderNodeData(String orderId) {
        String url = baseUrl + "/api/v2/order_node/" + orderId;
        log.info("Fetching order node data from butler_server | url: {}", url);
        try {
            String raw = restTemplate.getForObject(url, String.class);
            if (raw == null || raw.isBlank()) {
                log.warn("Empty response from butler_server order node API for orderId: {}", orderId);
                return null;
            }
            JsonNode node = objectMapper.readTree(raw);
            log.debug("Order node data fetched for orderId: {} | response: {}", orderId, raw);
            // Unwrap orderData wrapper if present
            JsonNode orderData = node.path("orderData");
            if (!orderData.isMissingNode()) {
                return orderData;
            }
            return node;
        } catch (Exception e) {
            log.warn("Failed to fetch order node data for orderId: {} — {}", orderId, e.getMessage());
            return null;
        }
    }
}
