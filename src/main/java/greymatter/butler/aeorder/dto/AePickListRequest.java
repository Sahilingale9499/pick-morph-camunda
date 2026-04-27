package greymatter.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
@Builder
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({"externalServiceRequestId", "serviceRequests", "fulfillmentArea", "is_deleted", "attributes", "on_hold", "stages", "type"})
@com.fasterxml.jackson.annotation.JsonIgnoreProperties({"deleted"})
public class AePickListRequest {

    private String externalServiceRequestId;
    private List<AeServiceRequestLine> serviceRequests;
    private List<String> fulfillmentArea;
    private AeTopLevelAttributes attributes;
    private String type;

    @JsonProperty("is_deleted")
    private boolean isDeleted;

    @JsonProperty("stages")
    private List<Map<String, Object>> stages;

    @JsonProperty("on_hold")
    private boolean onHold;

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"externalServiceRequestId", "expectations", "is_deleted", "attributes", "on_hold", "stages", "type"})
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties({"deleted"})
    public static class AeServiceRequestLine {
        private String externalServiceRequestId;
        private AeServiceRequestLineAttributes attributes;
        private AeExpectations expectations;
        private String type;

        @JsonProperty("is_deleted")
        private boolean isDeleted;

        @JsonProperty("stages")
        private List<Map<String, Object>> stages;

        @JsonProperty("on_hold")
        private boolean onHold;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class AeServiceRequestLineAttributes {
        @JsonProperty("extra_info")
        private ExtraInfo extraInfo;
        private AeLocation location;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class ExtraInfo {
        @JsonProperty("client_task_id")
        private String clientTaskId;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class AeLocation {
        private String displayName;
        private String fullAddress;
        private AeAddressFields addressFields;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class AeAddressFields {
        private String side;
        private String zone;
        private String level;
        private String bay;
        private String aisle;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class AeExpectations {
        private List<AeContainer> containers;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class AeContainer {
        private List<AeProduct> products;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"productAttributes", "productQuantity"})
    public static class AeProduct {
        private int productQuantity;
        private AeProductAttributes productAttributes;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"package_parameters", "filter_parameters", "product_sku", "barcodes", "lot_id"})
    public static class AeProductAttributes {
        @JsonProperty("lot_id")
        private String lotId;
        @JsonProperty("filter_parameters")
        private List<String> filterParameters;
        private List<String> barcodes;
        @JsonProperty("product_sku")
        private String productSku;
        @JsonProperty("package_parameters")
        private List<String> packageParameters;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"skipStandardPickProcess", "simple_priority", "order_options"})
    public static class AeTopLevelAttributes {
        @JsonProperty("skipStandardPickProcess")
        private boolean skipStandardPickProcess;
        @JsonProperty("order_options")
        private AeOrderOptions orderOptions;
        @JsonProperty("simple_priority")
        private String simplePriority;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"orderline_splitting", "customer_order_info", "destination_group", "simple_priority", "order_splitting", "order_clubbing", "container_type", "palletization", "grouping_tags", "behaviours", "bintags"})
    public static class AeOrderOptions {
        @JsonProperty("customer_order_info")
        private AeCustomerOrderInfo customerOrderInfo;
        private boolean palletization;
        @JsonProperty("order_clubbing")
        private boolean orderClubbing;
        @JsonProperty("order_splitting")
        private boolean orderSplitting;
        @JsonProperty("orderline_splitting")
        private boolean orderlineSplitting;
        @JsonProperty("grouping_tags")
        private AeGroupingTags groupingTags;
        private List<String> bintags;
        @JsonProperty("container_type")
        private String containerType;
        @JsonProperty("destination_group")
        private String destinationGroup;
        @JsonProperty("simple_priority")
        private String simplePriority;
        private List<String> behaviours;
    }

    @Data @Builder @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({"master_order_id", "order_line_id", "shipment_id", "order_id"})
    public static class AeCustomerOrderInfo {
        @JsonProperty("order_id")
        private String orderId;
        @JsonProperty("order_line_id")
        private String orderLineId;
        @JsonProperty("master_order_id")
        private String masterOrderId;
        @JsonProperty("shipment_id")
        private String shipmentId;
    }

    @Data @Builder
    @JsonPropertyOrder({"container_grouping_tag", "mission_grouning_tag", "bin_grouping_tag"})
    public static class AeGroupingTags {
        @JsonProperty("mission_grouning_tag")
        private String missionGrouningTag;
        @JsonProperty("container_grouping_tag")
        private String containerGroupingTag;
        @JsonProperty("bin_grouping_tag")
        private String binGroupingTag;
    }
}
