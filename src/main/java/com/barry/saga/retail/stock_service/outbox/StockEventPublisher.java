package com.barry.saga.retail.stock_service.outbox;

import com.barry.saga.retail.stock.event.StockRejectedEvent;
import com.barry.saga.retail.stock.event.StockReservedEvent;
import com.barry.saga.retail.stock_service.kafka.config.KafkaTopicsConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Publication des évènements "stock" via le pattern Transactional Outbox.
 *
 * Aucun appel Kafka ici : l'évènement est sérialisé en Avro puis inséré dans
 * la table outbox_event, dans la transaction de l'appelant (MANDATORY). Soit
 * la réservation ET l'évènement sont commités, soit aucun des deux. Debezium
 * lit ensuite le WAL Postgres et publie le message sur Kafka.
 */
@Component
@RequiredArgsConstructor
@Log4j2
public class StockEventPublisher {

    static final String AGGREGATE_TYPE = "StockReservation";
    static final String SCHEMA_VERSION = "v1";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaAvroSerializer avroSerializer;
    private final KafkaTopicsConfig properties;

    @Transactional(propagation = Propagation.MANDATORY)
    public void publishStockReserved(StockReservedEvent event) {
        save(properties.getTopics().getStockReserved(), event.getEventId(), event.getOrderId(), event);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void publishStockRejected(StockRejectedEvent event) {
        save(properties.getTopics().getStockRejected(), event.getEventId(), event.getOrderId(), event);
    }

    private void save(String topic, String eventId, String orderId, SpecificRecord event) {
        String eventType = event.getClass().getSimpleName();

        // Sérialisation identique à l'ancien KafkaTemplate (subject = <topic>-value) :
        // les consommateurs (KafkaAvroDeserializer) ne voient aucune différence.
        byte[] payload = avroSerializer.serialize(topic, event);

        outboxEventRepository.save(OutboxEventEntity.builder()
                .id(UUID.fromString(eventId))
                .aggregateType(AGGREGATE_TYPE)
                .aggregateId(orderId)
                .topic(topic)
                .eventType(eventType)
                .schemaVersion(SCHEMA_VERSION)
                .payload(payload)
                .createdAt(LocalDateTime.now())
                .build());

        log.info("📝 {} written to outbox (topic={}, key={})", eventType, topic, orderId);
    }
}
