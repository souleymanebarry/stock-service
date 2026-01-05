package com.barry.saga.retail.stock_service.repositories;

import com.barry.saga.retail.stock_service.entities.StockItemEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface StockItemRepository extends JpaRepository<StockItemEntity, UUID> {

    Optional<StockItemEntity> findBySku(String sku);

}
