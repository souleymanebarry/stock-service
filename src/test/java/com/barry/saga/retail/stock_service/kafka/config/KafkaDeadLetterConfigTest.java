package com.barry.saga.retail.stock_service.kafka.config;

import com.barry.saga.retail.order.event.OrderItem;
import com.barry.saga.retail.order.event.OrderPlacedEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Teste la DLQ de bout en bout sur un broker Kafka embarqué
 * (Schema Registry simulé via l'URL {@code mock://}).
 */
@EmbeddedKafka(partitions = 3, topics = {
        KafkaDeadLetterConfigTest.RETRYABLE_TOPIC,
        KafkaDeadLetterConfigTest.NOT_RETRYABLE_TOPIC,
        KafkaDeadLetterConfigTest.POISON_PILL_TOPIC
})
class KafkaDeadLetterConfigTest {

    static final String RETRYABLE_TOPIC = "dlt-test.retryable";
    static final String NOT_RETRYABLE_TOPIC = "dlt-test.not-retryable";
    static final String POISON_PILL_TOPIC = "dlt-test.poison-pill";

    private static final String SCHEMA_REGISTRY = "mock://dlt-test";
    private static final String GROUP_ID = "stock-service-group";

    private final EmbeddedKafkaBroker broker;
    private final AtomicInteger deliveries = new AtomicInteger();
    private ConcurrentMessageListenerContainer<String, SpecificRecord> container;

    KafkaDeadLetterConfigTest(EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    /** Comme en production : topics source à 3 partitions, DLT à 1 partition. */
    @BeforeAll
    static void createDltTopics(EmbeddedKafkaBroker broker) {
        broker.addTopics(
                new NewTopic(RETRYABLE_TOPIC + ".dlt", 1, (short) 1),
                new NewTopic(NOT_RETRYABLE_TOPIC + ".dlt", 1, (short) 1),
                new NewTopic(POISON_PILL_TOPIC + ".dlt", 1, (short) 1));
    }

    @BeforeEach
    void resetDeliveries() {
        deliveries.set(0);
    }

    @AfterEach
    void stopContainer() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    @DisplayName("Erreur de traitement : 3 rejeux puis DLT (Avro), offset commité")
    void processingError_isRetriedThenPublishedToDlt() {
        startListener(RETRYABLE_TOPIC, () -> {
            throw new IllegalStateException("database down");
        });
        OrderPlacedEvent event = orderPlacedEvent();

        send(RETRYABLE_TOPIC, event.getOrderId().toString(), event, new KafkaAvroSerializer());

        ConsumerRecord<String, Object> dlt = consumeDlt(RETRYABLE_TOPIC, avroDeserializerProps());
        assertThat(dlt.key()).isEqualTo(event.getOrderId().toString());
        assertThat(dlt.value()).isEqualTo(event);
        assertThat(header(dlt, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(RETRYABLE_TOPIC);
        assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(IllegalStateException.class.getName());
        assertThat(deliveries).hasValue(KafkaDeadLetterConfig.MAX_RETRIES + 1);
        assertOffsetCommitted(RETRYABLE_TOPIC);
    }

    @Test
    @DisplayName("Erreur non rejouable (IllegalArgumentException) : DLT sans rejeu")
    void notRetryableError_isPublishedToDltImmediately() {
        startListener(NOT_RETRYABLE_TOPIC, () -> {
            throw new IllegalArgumentException("Invalid UUID string: abc");
        });
        OrderPlacedEvent event = orderPlacedEvent();

        send(NOT_RETRYABLE_TOPIC, event.getOrderId().toString(), event, new KafkaAvroSerializer());

        ConsumerRecord<String, Object> dlt = consumeDlt(NOT_RETRYABLE_TOPIC, avroDeserializerProps());
        assertThat(dlt.value()).isEqualTo(event);
        assertThat(deliveries).hasValue(1);
        assertOffsetCommitted(NOT_RETRYABLE_TOPIC);
    }

    @Test
    @DisplayName("Poison pill (non Avro) : octets d'origine recopiés en DLT, listener jamais appelé")
    void poisonPill_isPublishedToDltAsRawBytes() {
        startListener(POISON_PILL_TOPIC, () -> { });
        byte[] garbage = "definitely-not-avro".getBytes(StandardCharsets.UTF_8);

        send(POISON_PILL_TOPIC, "key-1", garbage, new ByteArraySerializer());

        ConsumerRecord<String, Object> dlt = consumeDlt(POISON_PILL_TOPIC,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
        assertThat((byte[]) dlt.value()).isEqualTo(garbage);
        assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN)).isEqualTo(DeserializationException.class.getName());
        assertThat(deliveries).hasValue(0);
        assertOffsetCommitted(POISON_PILL_TOPIC);
    }

    // ----------------------------------------------------------------------

    /** Démarre un listener câblé exactement comme en production (factory + error handler). */
    private void startListener(String topic, Runnable behaviour) {
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(consumerConfig, "bootstrapServers", broker.getBrokersAsString());
        ReflectionTestUtils.setField(consumerConfig, "schemaRegistry", SCHEMA_REGISTRY);

        KafkaDeadLetterConfig deadLetterConfig = new KafkaDeadLetterConfig();
        ReflectionTestUtils.setField(deadLetterConfig, "bootstrapServers", broker.getBrokersAsString());
        ReflectionTestUtils.setField(deadLetterConfig, "schemaRegistry", SCHEMA_REGISTRY);

        container = consumerConfig
                .kafkaListenerContainerFactory(deadLetterConfig.kafkaErrorHandler())
                .createContainer(topic);
        container.setupMessageListener((AcknowledgingMessageListener<String, SpecificRecord>) (record, ack) -> {
            deliveries.incrementAndGet();
            behaviour.run();
            ack.acknowledge();
        });
        container.start();
    }

    private <V> void send(String topic, String key, V value, Serializer<V> valueSerializer) {
        valueSerializer.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY), false);
        Map<String, Object> props = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        try (KafkaProducer<String, V> producer = new KafkaProducer<>(props, new StringSerializer(), valueSerializer)) {
            producer.send(new ProducerRecord<>(topic, key, value));
        }
    }

    private ConsumerRecord<String, Object> consumeDlt(String sourceTopic, Map<String, Object> valueProps) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("dlt-reader-" + UUID.randomUUID(), "false", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(valueProps);
        try (Consumer<String, Object> consumer = new DefaultKafkaConsumerFactory<String, Object>(props).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, sourceTopic + ".dlt");
            return KafkaTestUtils.getSingleRecord(consumer, sourceTopic + ".dlt", Duration.ofSeconds(30));
        }
    }

    private Map<String, Object> avroDeserializerProps() {
        return Map.of(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    /** L'offset du message en échec doit être commité : la partition n'est plus bloquée. */
    private void assertOffsetCommitted(String topic) {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                Map<TopicPartition, OffsetAndMetadata> offsets = admin
                        .listConsumerGroupOffsets(GROUP_ID).partitionsToOffsetAndMetadata().get();
                long committed = offsets.entrySet().stream()
                        .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
                        .mapToLong(e -> e.getValue().offset())
                        .sum();
                assertThat(committed).isEqualTo(1L);
            });
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static OrderPlacedEvent orderPlacedEvent() {
        return OrderPlacedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setIdempotencyKey(UUID.randomUUID().toString())
                .setOrderId(UUID.randomUUID().toString())
                .setCustomerId("CUST-1")
                .setTotalAmount(new BigDecimal("899.00"))
                .setCreatedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .setItems(List.of(OrderItem.newBuilder()
                        .setSku("SKU-PIXEL9")
                        .setQuantity(1)
                        .setUnitPrice(new BigDecimal("899.00"))
                        .build()))
                .build();
    }
}
