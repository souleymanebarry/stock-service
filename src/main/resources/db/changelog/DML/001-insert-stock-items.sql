--liquibase formatted sql
--changeset sbarry:001-insert-stock-items

-- SKU alignés sur le catalogue (catalog-service). Quelques stocks faibles ou nuls
-- permettent de tester le chemin d'échec du Saga (StockRejectedEvent).
INSERT INTO stock_items (stock_id, sku, product_name, available_quantity, reserved_quantity, updated_at) VALUES
    ('80b1bfa6-db79-44b0-ba4c-850f3dd2de68', 'SKU-IPH15P',    'iPhone 15 Pro',                    50,  0, NOW()),
    ('4b2ab035-8e28-4290-a96c-2c92d06f1b35', 'SKU-MBAM3',     'MacBook Air M3',                   20,  0, NOW()),
    ('537ba456-4252-4642-a9f0-5f0b3087b646', 'SKU-PS5SLIM',   'PlayStation 5 Slim',               5,   0, NOW()),
    ('c4377e52-18e4-4108-992e-17974d253432', 'SKU-NSWOLED',   'Nintendo Switch OLED',             30,  0, NOW()),
    ('5230b883-e6cb-4fcf-a8f5-e3083f3f47d3', 'SKU-SGS24',     'Samsung Galaxy S24',               40,  0, NOW()),
    ('197f98c2-cb29-42f1-a59e-3028492fa9ec', 'SKU-XPS15',     'Dell XPS 15',                      2,   0, NOW()),
    ('800764f2-dd00-4526-9de2-2d5bf8ea6379', 'SKU-APP2',      'AirPods Pro 2',                    100, 0, NOW()),
    ('0894cdaf-0ca0-40da-9227-0d66fb90a97a', 'SKU-WH1000XM5', 'Sony WH-1000XM5',                  25,  0, NOW()),
    ('170e350d-cf1b-4a8a-9226-304704a36738', 'SKU-PIXEL9',    'Google Pixel 9',                   0,   0, NOW()),
    ('a09d3c80-a8b1-4e9e-8659-c2ddd3b2ac47', 'SKU-MXM3S',     'Logitech MX Master 3S',            60,  0, NOW()),
    ('2489a671-015d-4841-b33f-698af982007c', 'SKU-HLL2350DW', 'Brother HL-L2350DW Laser Printer', 10,  0, NOW()),
    ('9004477e-d47a-4368-9a98-8b153010eacb', 'SKU-LG27-4K',   'LG 27'' 4K Monitor',               15,  0, NOW());
--rollback DELETE FROM stock_items;
