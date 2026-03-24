package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import java.util.List;

/**
 * DTO for messages consumed from the "pick-instruction.requests" Kafka topic.
 * Published by butler_server when a new pick instruction is created.
 * Used as the workflow input — carries all data needed to construct the AE order
 * payload and drive subsequent gRPC calls.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PickInstructionRequestMessage {

    private String id;                  // pick_instruction_id = AE externalServiceRequestId (PK)
    private String orderId;             // normal/main order ID → AE customerOrderInfo.order_id
    private String orderlineId;         // normal/main orderline ID → AE customerOrderInfo.order_line_id
    private int qty;                    // pick quantity → AE productQuantity
    private String slotId;              // slot reference → gRPC slotref
    private String uom;                 // unit of measure → AE package_parameters
    private int tpid;                   // product type ID (integer) — gRPC use; NOT the same as product SKU
    private List<String> barcodes;      // scannable barcodes → AE productAttributes.barcodes
    private int ppsId;                  // PPS station ID → gRPC
    private String binId;               // bin ID within PPS → gRPC
}
