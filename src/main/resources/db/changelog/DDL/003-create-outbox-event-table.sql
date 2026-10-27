--liquibase formatted sql
--changeset sbarry:003-create-outbox-event-table

-- Table outbox lue par Debezium (CDC) : chaque INSERT devient un message Kafka.
-- payload = octets Avro au format Confluent (magic byte + schema id + données),
-- recopiés tels quels sur le topic par le connecteur (cf. saga-pattern/infra/connectors).
CREATE TABLE outbox_event (
    id             UUID         NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    schema_version VARCHAR(50)  NOT NULL,
    payload        BYTEA        NOT NULL,
    created_at     TIMESTAMP    NOT NULL,
    CONSTRAINT pk_outbox_event PRIMARY KEY (id)
);
--rollback DROP TABLE outbox_event;
