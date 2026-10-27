--liquibase formatted sql
--changeset sbarry:001-create-stock-items-table

CREATE TABLE stock_items (
    stock_id           UUID         NOT NULL,
    sku                VARCHAR(255) NOT NULL,
    product_name       VARCHAR(255),
    available_quantity INTEGER      NOT NULL,
    reserved_quantity  INTEGER      NOT NULL,
    updated_at         TIMESTAMP,
    CONSTRAINT pk_stock_items PRIMARY KEY (stock_id),
    CONSTRAINT uk_stock_items_sku UNIQUE (sku),
    CONSTRAINT ck_stock_items_available CHECK (available_quantity >= 0),
    CONSTRAINT ck_stock_items_reserved CHECK (reserved_quantity >= 0)
);
--rollback DROP TABLE stock_items;
