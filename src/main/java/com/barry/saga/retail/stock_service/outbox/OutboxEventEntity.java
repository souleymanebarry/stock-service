package com.barry.saga.retail.stock_service.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Ligne de la table outbox. Elle est écrite dans la même transaction que
 * la réservation de stock ; Debezium la transforme ensuite en message Kafka.
 */
@Entity
@Table(name = "outbox_event")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEventEntity {

    /** = eventId de l'évènement Avro (exposé en header "id" par Debezium). */
    @Id
    private UUID id;

    private String aggregateType;

    /** Clé Kafka (orderId) : garantit l'ordre par commande. */
    private String aggregateId;

    /** Topic cible, utilisé par l'EventRouter Debezium pour le routage. */
    private String topic;

    private String eventType;

    private String schemaVersion;

    /** Octets Avro au format Confluent (magic byte + schema id + données). */
    @Column(columnDefinition = "bytea")
    private byte[] payload;

    private LocalDateTime createdAt;
}
