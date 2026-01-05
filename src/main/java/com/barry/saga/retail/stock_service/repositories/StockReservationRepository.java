package com.barry.saga.retail.stock_service.repositories;

import com.barry.saga.retail.stock_service.entities.StockReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface StockReservationRepository extends JpaRepository<StockReservationEntity , UUID> {

}
