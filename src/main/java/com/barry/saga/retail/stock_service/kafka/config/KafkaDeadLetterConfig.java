package com.barry.saga.retail.stock_service.kafka.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.log4j.Log4j2;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.barry.saga.retail.stock_service.kafka.config.KafkaTopicsConfig.DLT_SUFFIX;

/**
 * Dead Letter Queue des consommateurs Kafka.
 * <p>
 * Un message en échec est rejoué {@value #MAX_RETRIES} fois (backoff exponentiel 1s → 2s → 4s),
 * puis publié sur {@code <topic>.dlt} et son offset est commité :
 * la consommation reprend au lieu de bloquer la partition.
 * <ul>
 *     <li>Erreurs non rejouables (désérialisation, {@link IllegalArgumentException}…) : DLT immédiatement</li>
 *     <li>Message illisible (poison pill) : les octets d'origine sont recopiés tels quels</li>
 *     <li>Message Avro lisible : ré-sérialisé en Avro</li>
 * </ul>
 * Spring ajoute les headers {@code kafka_dlt-original-topic/partition/offset} et
 * {@code kafka_dlt-exception-fqcn/message/stacktrace} pour le diagnostic et le rejeu.
 * <p>
 * La DLT est une affaire d'infrastructure : elle est publiée directement sur Kafka,
 * pas via l'outbox (aucun état métier à garder cohérent avec la base).
 */
@Configuration
@Log4j2
public class KafkaDeadLetterConfig {

    static final int MAX_RETRIES = 3;
    private static final long INITIAL_INTERVAL_MS = 1_000L;
    private static final double MULTIPLIER = 2.0;
    private static final long MAX_INTERVAL_MS = 10_000L;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistry;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        backOff.setInitialInterval(INITIAL_INTERVAL_MS);
        backOff.setMultiplier(MULTIPLIER);
        backOff.setMaxInterval(MAX_INTERVAL_MS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer(), backOff);
        // Données invalides (ex. orderId non UUID) : rejouer ne changerait rien
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setRetryListeners(loggingRetryListener());
        return errorHandler;
    }

    private DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        // Le template est choisi selon le type de la valeur (ordre significatif)
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesDltTemplate());
        templates.put(SpecificRecord.class, avroDltTemplate());

        // Partition -1 : pas de partition imposée, Kafka la choisit à partir de la clé (orderId).
        // La DLT n'a donc pas à suivre le nombre de partitions du topic source.
        return new DeadLetterPublishingRecoverer(templates,
                (consumerRecord, exception) ->
                        new TopicPartition(consumerRecord.topic() + DLT_SUFFIX, -1));
    }

    /** Poison pills : la valeur n'a pas pu être désérialisée, on renvoie les octets bruts. */
    @Bean
    public KafkaTemplate<String, byte[]> bytesDltTemplate() {
        Map<String, Object> props = dltProducerProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /** Événements Avro lisibles mais dont le traitement a échoué. */
    @Bean
    public KafkaTemplate<String, SpecificRecord> avroDltTemplate() {
        Map<String, Object> props = dltProducerProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    private Map<String, Object> dltProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Un message envoyé en DLT ne doit pas être perdu : ack de tous les réplicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    private RetryListener loggingRetryListener() {
        return new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("⚠ Kafka delivery failed | topic={} | partition={} | offset={} | attempt={}/{} | error={}",
                        record.topic(), record.partition(), record.offset(),
                        deliveryAttempt, MAX_RETRIES + 1, ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("☠ Message sent to DLT {}{} | source: topic={} | partition={} | offset={} | key={}",
                        record.topic(), DLT_SUFFIX, record.topic(), record.partition(), record.offset(), record.key(), ex);
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("❌ DLT publication failed, record will be retried | topic={} | partition={} | offset={}",
                        record.topic(), record.partition(), record.offset(), failure);
            }
        };
    }
}
