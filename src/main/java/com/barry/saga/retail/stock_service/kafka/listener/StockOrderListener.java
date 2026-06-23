package com.barry.saga.retail.stock_service.kafka.listener;

import com.barry.saga.retail.order.event.OrderPlacedEvent;
import com.barry.saga.retail.stock_service.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Log4j2
public class StockOrderListener {

    private final StockService stockService;

    @KafkaListener(
            topics = "${spring.kafka.topics.order-placed}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderPlaced(OrderPlacedEvent orderPlacedEvent, Acknowledgment ack) {
        stockService.reservedStock(orderPlacedEvent);
        log.info("📩 Received OrderPlacedEvent for orderId={}", orderPlacedEvent.getOrderId());
        ack.acknowledge();
    }
}