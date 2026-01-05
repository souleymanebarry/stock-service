package com.barry.saga.retail.stock_service.share.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * envoyé au order-service
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StockReservedEvent {

    @Builder.Default
    private String eventType = "StockReserved";

    private UUID orderId;

    private List<StockItem> items;

    private LocalDateTime reservedAt;

    @Data
    @Builder
    public static class StockItem {
        private String sku;
        private Integer quantity;
    }
}
