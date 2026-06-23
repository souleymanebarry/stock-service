package com.barry.saga.retail.stock_service.kafka;

import com.barry.saga.retail.stock.event.StockRejectedEvent;
import com.barry.saga.retail.stock.event.StockReservedEvent;
import com.barry.saga.retail.stock_service.kafka.config.KafkaTopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
@Log4j2
public class StockEventProducer {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final KafkaTopicsConfig properties;


    /**
     * Publie un événement de réservation de stock réussie.
     */
    public void sendStockReserved(StockReservedEvent event) {
        send(properties.getTopics().getStockReserved(), event.getOrderId(), event);
        log.info("📦 StockReservedEvent sent | orderId={}", event.getOrderId());
    }

    /**
     * Publie un événement de rejet de réservation de stock.
     */
    public void sendStockRejected(StockRejectedEvent event) {
        send(properties.getTopics().getStockRejected(), event.getOrderId(), event);
        log.info("❌ StockRejectedEvent sent | orderId={}", event.getOrderId());
    }

    // ===============================
    // Méthode centrale d’envoi Kafka
    // ===============================
    private void send(String topic, String key, SpecificRecord event) {

        ProducerRecord<String, SpecificRecord> record =
                new ProducerRecord<>(topic, key, event);

        // Headers standards Saga / Event-driven
        record.headers().add("eventType", event.getClass().getSimpleName().getBytes(StandardCharsets.UTF_8));
        record.headers().add("schemaVersion", "v1".getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("❌ Failed to send event {} to topic {}",
                                event.getClass().getSimpleName(), topic, ex);
                    } else {
                        log.info("📤 {} sent to topic {} (key={}, partition={}, offset={})",
                                event.getClass().getSimpleName(),
                                topic,
                                key,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}