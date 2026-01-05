package com.barry.saga.retail.stock_service.entities;

import com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
 * Pour garder une trace(Historique) des réservations (important dans un Saga)
 */

@Entity
@Table(name = "stock_reservations")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockReservationEntity {

    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID reservationId;

    private UUID orderId;

    private String sku;

    private Integer quantity;

    @Enumerated(EnumType.STRING)
    private StockReservationStatus status;

    private LocalDateTime createdAt;
}
