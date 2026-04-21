package greymatter.butler.aeorder.service;

import greymatter.butler.aeorder.dto.AePickListRequest;
import greymatter.butler.aeorder.dto.PickInstruction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AeOrderBuilderService#buildProductAttributes}.
 * Pure static logic — no mocks, no Spring context.
 */
class AeOrderBuilderServiceTest {

    private PickInstruction msg() {
        PickInstruction msg = new PickInstruction();
        msg.setUom("EACH");
        msg.setTpid(42);
        return msg;
    }

    @Test
    void extracts_product_sku_from_filter_params() {
        List<String> fp = List.of("product_sku = 'SKU-123'");
        List<String> pp = List.of("package_name = 'BOX'");
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), fp, pp);
        assertEquals("SKU-123", result.getProductSku());
        assertEquals(pp, result.getPackageParameters());
        assertEquals("42", result.getLotId());
    }

    @Test
    void product_sku_is_null_when_no_matching_param() {
        List<String> fp = List.of("item_id = 'X'");
        List<String> pp = List.of("package_name = 'BOX'");
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), fp, pp);
        assertNull(result.getProductSku());
    }

    @Test
    void product_sku_is_null_when_filter_params_null() {
        List<String> pp = List.of("package_name = 'BOX'");
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), null, pp);
        assertNull(result.getProductSku());
    }

    @Test
    void product_sku_is_null_when_filter_params_empty() {
        List<String> pp = List.of("package_name = 'BOX'");
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), List.of(), pp);
        assertNull(result.getProductSku());
    }

    @Test
    void empty_sku_between_quotes_returns_empty_string() {
        List<String> fp = List.of("product_sku = ''");
        List<String> pp = List.of("package_name = 'BOX'");
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), fp, pp);
        assertEquals("", result.getProductSku());
    }

    @Test
    void package_params_fallback_when_null() {
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), null, null);
        assertEquals(List.of("package_name = 'EACH'"), result.getPackageParameters());
    }

    @Test
    void package_params_fallback_when_empty() {
        AePickListRequest.AeProductAttributes result =
                AeOrderBuilderService.buildProductAttributes(msg(), null, List.of());
        assertEquals(List.of("package_name = 'EACH'"), result.getPackageParameters());
    }
}
