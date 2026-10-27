package com.barry.saga.retail.stock_service.outbox;

import com.barry.saga.retail.stock.event.StockRejectedEvent;
import com.barry.saga.retail.stock.event.StockReservedEvent;
import com.barry.saga.retail.stock_service.kafka.config.KafkaTopicsConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StockEventPublisherTest {

    private static final String ORDER_ID = "order-1";
    private static final String EVENT_ID = UUID.randomUUID().toString();
    private static final byte[] PAYLOAD = {0, 0, 0, 0, 1, 42};

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final KafkaAvroSerializer avroSerializer = mock(KafkaAvroSerializer.class);
    private StockEventPublisher publisher;

    @BeforeEach
    void setUp() {
        KafkaTopicsConfig properties = new KafkaTopicsConfig();
        properties.getTopics().setStockReserved("stock.reserved");
        properties.getTopics().setStockRejected("stock.rejected");
        publisher = new StockEventPublisher(outboxEventRepository, avroSerializer, properties);

        when(avroSerializer.serialize(anyString(), any())).thenReturn(PAYLOAD);
    }

    @Test
    @DisplayName("publishStockReserved écrit l'évènement dans l'outbox pour le topic stock.reserved")
    void publishStockReserved_writesOutboxRow() {
        StockReservedEvent event = new StockReservedEvent();
        event.setEventId(EVENT_ID);
        event.setOrderId(ORDER_ID);

        publisher.publishStockReserved(event);

        assertStored("stock.reserved", event);
    }

    @Test
    @DisplayName("publishStockRejected écrit l'évènement dans l'outbox pour le topic stock.rejected")
    void publishStockRejected_writesOutboxRow() {
        StockRejectedEvent event = new StockRejectedEvent();
        event.setEventId(EVENT_ID);
        event.setOrderId(ORDER_ID);

        publisher.publishStockRejected(event);

        assertStored("stock.rejected", event);
    }

    /**
     * Vérifie la sérialisation Avro pour le bon topic et le contenu de la ligne outbox :
     * id = eventId, clé = orderId (ordre par partition), type et version de l'évènement.
     */
    private void assertStored(String topic, SpecificRecord event) {
        verify(avroSerializer).serialize(topic, event);

        ArgumentCaptor<OutboxEventEntity> captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository).save(captor.capture());

        OutboxEventEntity row = captor.getValue();
        assertThat(row.getId()).isEqualTo(UUID.fromString(EVENT_ID));
        assertThat(row.getAggregateType()).isEqualTo("StockReservation");
        assertThat(row.getAggregateId()).isEqualTo(ORDER_ID);
        assertThat(row.getTopic()).isEqualTo(topic);
        assertThat(row.getEventType()).isEqualTo(event.getClass().getSimpleName());
        assertThat(row.getSchemaVersion()).isEqualTo("v1");
        assertThat(row.getPayload()).isEqualTo(PAYLOAD);
        assertThat(row.getCreatedAt()).isNotNull();
    }
}
