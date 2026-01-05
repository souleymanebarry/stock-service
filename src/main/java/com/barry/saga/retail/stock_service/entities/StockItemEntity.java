package com.barry.saga.retail.stock_service.entities;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Réprésente le stock disponible pour chaque SKU
 */

@Entity
@Table(name = "stock_items")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockItemEntity {

    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID stockId;

    private String sku;

    private String productName;

    private Integer availableQuantity;

    private Integer reservedQuantity;

    private LocalDateTime updatedAt;
}
