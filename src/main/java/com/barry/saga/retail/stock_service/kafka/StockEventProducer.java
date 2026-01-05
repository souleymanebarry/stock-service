package com.barry.saga.retail.stock_service.kafka;

import com.barry.saga.retail.stock_service.kafka.config.KafkaTopicsConfig;
import com.barry.saga.retail.stock_service.share.event.StockRejectedEvent;
import com.barry.saga.retail.stock_service.share.event.StockReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Log4j2
public class StockEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig properties;


    /**
     * Publie un événement de réservation de stock réussie.
     */
    public void sendStockReserved(StockReservedEvent stockReservedEvent) {
        String topic = properties.getTopics().getStockReserved();
        kafkaTemplate.send(topic, stockReservedEvent);
        log.info("📦 StockReservedEvent sent | orderId={} | topic={}", stockReservedEvent.getOrderId(),topic);
    }

    /**
     * Publie un événement de rejet de réservation de stock.
     */
    public void sendStockRejected(StockRejectedEvent stockRejectedEvent) {
        String topic = properties.getTopics().getStockRejected();
        kafkaTemplate.send(topic, stockRejectedEvent);
        log.info("❌ StockRejectedEvent sent | orderId={} | topic={}", stockRejectedEvent.getOrderId(), topic);
    }
}
