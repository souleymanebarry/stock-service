package com.barry.saga.retail.stock_service.outbox;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class OutboxConfig {

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistry;

    /**
     * Sérialiseur Avro utilisé hors producer Kafka pour remplir la colonne payload.
     * Il enregistre le schéma dans le Schema Registry comme le faisait le producer.
     */
    @Bean
    public KafkaAvroSerializer outboxAvroSerializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry), false);
        return serializer;
    }
}
