package com.barry.saga.retail.stock_service.kafka.listener;

import com.barry.saga.retail.stock_service.service.StockService;
import com.barry.saga.retail.stock_service.share.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Log4j2
public class StockOrderListener {

    private final StockService stockService;

    @KafkaListener(
            topics = "order.placed",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderPlaced(OrderPlacedEvent orderPlacedEvent) {
        stockService.reservedStock(orderPlacedEvent);
        log.info("📩 Received OrderPlacedEvent for orderId={}", orderPlacedEvent.getOrderId());
    }
}
