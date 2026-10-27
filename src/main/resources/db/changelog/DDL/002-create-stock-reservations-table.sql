--liquibase formatted sql
--changeset sbarry:002-create-stock-reservations-table

CREATE TABLE stock_reservations (
    reservation_id UUID         NOT NULL,
    order_id       UUID         NOT NULL,
    sku            VARCHAR(255) NOT NULL,
    quantity       INTEGER      NOT NULL,
    status         VARCHAR(50)  NOT NULL,
    created_at     TIMESTAMP,
    CONSTRAINT pk_stock_reservations PRIMARY KEY (reservation_id)
);

CREATE INDEX idx_stock_reservations_order_id ON stock_reservations (order_id);
--rollback DROP TABLE stock_reservations;
