package com.barry.saga.retail.stock_service.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.lang.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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
 * {@code kafka_dlt-exception-fqcn/message/stacktrace} pour le diagnostic et le rejeu,
 * et on y ajoute {@value #DELIVERY_ATTEMPTS_HEADER} (nombre de tentatives).
 * <p>
 * La DLT est une affaire d'infrastructure : elle est publiée directement sur Kafka,
 * pas via l'outbox (aucun état métier à garder cohérent avec la base).
 */
@Configuration
@Log4j2
@RequiredArgsConstructor
public class KafkaDeadLetterConfig implements DisposableBean {

    /** Nombre de tentatives avant l'envoi en DLT, en texte pour être lisible dans Conduktor. */
    public static final String DELIVERY_ATTEMPTS_HEADER = "dlt-delivery-attempts";

    static final int MAX_RETRIES = 3;

    /**
     * Pas de partition imposée : Kafka la choisit à partir de la clé (orderId).
     * La DLT n'a donc pas à suivre le nombre de partitions du topic source.
     */
    private static final int ANY_PARTITION = -1;
    private static final long INITIAL_INTERVAL_MS = 1_000L;
    private static final double MULTIPLIER = 2.0;
    private static final long MAX_INTERVAL_MS = 10_000L;

    private final KafkaProperties kafkaProperties;
    private final List<DefaultKafkaProducerFactory<?, ?>> dltProducerFactories = new CopyOnWriteArrayList<>();

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        backOff.setInitialInterval(INITIAL_INTERVAL_MS);
        backOff.setMultiplier(MULTIPLIER);
        backOff.setMaxInterval(MAX_INTERVAL_MS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer(), backOff);
        // Données invalides (ex. orderId non UUID) : rejouer ne changerait rien
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setRetryListeners(new LoggingRetryListener());
        return errorHandler;
    }

    /** Ferme les producteurs DLT (flush des envois en cours) à l'arrêt du contexte. */
    @Override
    public void destroy() {
        dltProducerFactories.forEach(DefaultKafkaProducerFactory::destroy);
    }

    private DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        // Le template est choisi selon le type de la valeur (ordre significatif)
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dltTemplate(ByteArraySerializer.class));
        templates.put(SpecificRecord.class, dltTemplate(KafkaAvroSerializer.class));

        return new DeadLetterPublishingRecoverer(templates,
                (consumerRecord, exception) ->
                        new TopicPartition(consumerRecord.topic() + DLT_SUFFIX, ANY_PARTITION));
    }

    /**
     * Templates réservés à la DLT, volontairement pas exposés en beans : un bean {@code KafkaTemplate}
     * désactiverait celui auto-configuré par Spring Boot et risquerait d'être injecté ailleurs.
     * <ul>
     *     <li>{@link ByteArraySerializer} : poison pills, les octets bruts sont recopiés</li>
     *     <li>{@link KafkaAvroSerializer} : événements Avro lisibles dont le traitement a échoué</li>
     * </ul>
     */
    private <V> KafkaTemplate<String, V> dltTemplate(Class<? extends Serializer<V>> valueSerializer) {
        Map<String, Object> props = dltProducerProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        DefaultKafkaProducerFactory<String, V> producerFactory = new DefaultKafkaProducerFactory<>(props);
        dltProducerFactories.add(producerFactory);
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Base = configuration Spring Boot ({@code spring.kafka.*}) : bootstrap-servers et
     * {@code spring.kafka.properties.*} (dont schema.registry.url).
     */
    private Map<String, Object> dltProducerProps() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Un message envoyé en DLT ne doit pas être perdu : ack de tous les réplicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    /** Trace chaque échec, ajoute le header du nombre de tentatives, et logue l'issue de la DLT. */
    private static final class LoggingRetryListener implements RetryListener {

        @Override
        public void failedDelivery(@NonNull ConsumerRecord<?, ?> consumerRecord, @NonNull Exception ex,
                                   int deliveryAttempt) {
            // Appelé avant la publication en DLT, sur le même record : le recoverer recopie le header
            consumerRecord.headers().remove(DELIVERY_ATTEMPTS_HEADER);
            consumerRecord.headers().add(DELIVERY_ATTEMPTS_HEADER,
                    String.valueOf(deliveryAttempt).getBytes(StandardCharsets.UTF_8));
            log.warn("⚠ Kafka delivery failed | topic={} | partition={} | offset={} | attempt={}/{} | error={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                    deliveryAttempt, MAX_RETRIES + 1, ex.getMessage());
        }

        @Override
        public void recovered(@NonNull ConsumerRecord<?, ?> consumerRecord, @NonNull Exception ex) {
            log.error("☠ Message sent to DLT {}{} | source: topic={} | partition={} | offset={} | key={}",
                    consumerRecord.topic(), DLT_SUFFIX, consumerRecord.topic(), consumerRecord.partition(),
                    consumerRecord.offset(), consumerRecord.key(), ex);
        }

        @Override
        public void recoveryFailed(@NonNull ConsumerRecord<?, ?> consumerRecord, @NonNull Exception original,
                                   @NonNull Exception failure) {
            log.error("❌ DLT publication failed, record will be retried | topic={} | partition={} | offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), failure);
        }
    }
}
