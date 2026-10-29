package com.barry.saga.retail.stock_service.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
@Getter
@Setter
public class KafkaTopicsConfig {

    // Un seul broker en local (cf. docker-compose) → réplication = 1.
    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1;
    private static final int DLT_PARTITIONS = 1;

    /** Suffixe des Dead Letter Topics : order.placed → order.placed.dlt */
    public static final String DLT_SUFFIX = ".dlt";

    private Topics topics = new Topics();

    @Getter @Setter
    public static class Topics {
        private String stockReserved;
        private String stockRejected;
        private String orderPlaced;
    }

    // ------------------------------------------------------------------
    // Provisioning des topics du domaine "stock" (stock-service = owner).
    // Sans ces beans, c'est Debezium qui les créerait à la volée avec la
    // config par défaut du broker (1 partition). La clé Kafka = orderId
    // garantit l'ordre par commande quel que soit le nombre de partitions.
    // ------------------------------------------------------------------

    @Bean
    public NewTopic stockReservedTopic() {
        return TopicBuilder.name(topics.getStockReserved())
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic stockRejectedTopic() {
        return TopicBuilder.name(topics.getStockRejected())
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    // ------------------------------------------------------------------
    // Dead Letter Topic du topic CONSOMMÉ par stock-service.
    // C'est le consommateur qui y écrit (DeadLetterPublishingRecoverer),
    // il en est donc l'owner. 1 seule partition : volume faible, lecture
    // manuelle, et les messages restent dans leur ordre d'arrivée
    // (la partition d'origine est conservée dans le header kafka_dlt-original-partition).
    // ------------------------------------------------------------------

    @Bean
    public NewTopic orderPlacedDltTopic() {
        return TopicBuilder.name(topics.getOrderPlaced() + DLT_SUFFIX)
                .partitions(DLT_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
}
