package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.system.support.SystemApi;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Test 5: the full pipeline, end to end, through the public API of the real system. */
class HappyPathSystemIT extends AbstractSystemTest {

    @Test
    @DisplayName(
            "REQ-FUNC-004..015 test 5: an order with stock is CONFIRMED and its stock is decremented exactly once")
    void orderWithStockIsConfirmed() {
        String token = token();
        int stockBefore = DATABASE.stockOf("SKU-001");

        HttpResponse<String> created = API.createOrder(token, "SKU-001", 2);

        assertThat(created.statusCode()).isEqualTo(201);
        String orderId = SystemApi.orderIdOf(created);
        awaitStatus(token, orderId, "CONFIRMED");
        assertThat(stockBefore - DATABASE.stockOf("SKU-001")).isEqualTo(2);
    }

    @Test
    @DisplayName(
            "REQ-FUNC-013 REQ-FUNC-015 test 5: an order for a product without stock is REJECTED")
    void orderWithoutStockIsRejected() {
        String token = token();

        HttpResponse<String> created = API.createOrder(token, "SKU-003", 1);

        assertThat(created.statusCode()).isEqualTo(201);
        awaitStatus(token, SystemApi.orderIdOf(created), "REJECTED");
        assertThat(DATABASE.stockOf("SKU-003")).isZero();
    }

    @Test
    @DisplayName(
            "REQ-FUNC-013 test 5: a catalogued product without a stock row is REJECTED (UNKNOWN_PRODUCT)")
    void catalogedProductWithoutStockRowIsRejected() {
        String token = token();

        HttpResponse<String> created = API.createOrder(token, "SKU-004", 1);

        assertThat(created.statusCode()).isEqualTo(201);
        awaitStatus(token, SystemApi.orderIdOf(created), "REJECTED");
    }

    @Test
    @DisplayName("ADR-0014 test 5: a product outside the catalog is refused upfront with 422")
    void productOutsideCatalogIsUnprocessable() {
        assertThat(API.createOrder(token(), "SKU-999", 1).statusCode()).isEqualTo(422);
    }
}
