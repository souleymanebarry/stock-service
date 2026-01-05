package com.barry.saga.retail.stock_service.share.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * OrderPacedEvent (reçu du order-service)
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderPlacedEvent {

    private String eventType;

    private UUID orderId;

    private String customerId;

    private List<Item> items;

    private BigDecimal totalAmount;

    private LocalDateTime createdAt;

    private String idempotencyKey;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Item {
        private String sku;

        private Integer quantity;

        private BigDecimal unitPrice;
    }

}
