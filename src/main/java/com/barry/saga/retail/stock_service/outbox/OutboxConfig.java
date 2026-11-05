package com.barry.saga.retail.stock_service.outbox;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class OutboxConfig {

    private final KafkaProperties kafkaProperties;

    /**
     * Sérialiseur Avro utilisé hors producer Kafka pour remplir la colonne payload.
     * Il enregistre le schéma dans le Schema Registry comme le faisait le producer.
     * <p>
     * Configuré avec {@code spring.kafka.properties.*} (schema.registry.url et, le cas échéant,
     * l'authentification au Schema Registry), comme les producers et consumers Kafka.
     */
    @Bean
    public KafkaAvroSerializer outboxAvroSerializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.copyOf(kafkaProperties.getProperties()), false);
        return serializer;
    }
}
