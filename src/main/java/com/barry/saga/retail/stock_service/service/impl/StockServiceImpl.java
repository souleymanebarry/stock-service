package com.barry.saga.retail.stock_service.service.impl;

import com.barry.saga.retail.order.event.OrderItem;
import com.barry.saga.retail.order.event.OrderPlacedEvent;
import com.barry.saga.retail.stock.event.StockItem;
import com.barry.saga.retail.stock.event.StockRejectedEvent;
import com.barry.saga.retail.stock.event.StockReservedEvent;
import com.barry.saga.retail.stock_service.entities.StockItemEntity;
import com.barry.saga.retail.stock_service.entities.StockReservationEntity;
import com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus;
import com.barry.saga.retail.stock_service.outbox.StockEventPublisher;
import com.barry.saga.retail.stock_service.repositories.StockItemRepository;
import com.barry.saga.retail.stock_service.repositories.StockReservationRepository;
import com.barry.saga.retail.stock_service.service.StockService;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus.REJECTED;
import static com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus.RESERVED;

@Service
@RequiredArgsConstructor
@Log4j2
public class StockServiceImpl implements StockService {

    private final StockItemRepository stockItemRepository;
    private final StockReservationRepository stockReservationRepository;
    private final StockEventPublisher stockEventPublisher;

    /**
     * Traite un événement {@link OrderPlacedEvent} reçu depuis Kafka.
     * <p>
     * Cette méthode implémente la logique de réservation de stock dans le cadre
     * d’une saga, en "tout ou rien" :
     * <ul>
     *     <li>Ignore une commande déjà traitée (livraison Kafka/Debezium "at-least-once")</li>
     *     <li>Vérifie d'abord la disponibilité de TOUS les items, sans rien modifier</li>
     *     <li>Au premier item introuvable ou insuffisant : rejet, aucun stock n'est touché</li>
     *     <li>Sinon réserve toutes les quantités</li>
     *     <li>Écrit un {@code StockReservedEvent} ou {@code StockRejectedEvent} dans l'outbox</li>
     * </ul>
     *
     * Le stock, l'historique des réservations et l'évènement sont écrits dans la
     * même transaction : soit tout est validé, soit rien.
     *
     * @param orderPlacedEvent événement de commande contenant les items à réserver
     */
    @Override
    @Transactional
    public void reservedStock(OrderPlacedEvent orderPlacedEvent) {
        UUID orderId = UUID.fromString(orderPlacedEvent.getOrderId());

        // 0. Idempotence : une commande déjà traitée (réservée OU rejetée) est ignorée
        if (stockReservationRepository.existsByOrderId(orderId)) {
            log.info("♻ OrderPlacedEvent already processed for orderId={} — skipping", orderId);
            return;
        }

        // 1. Vérification de tous les items AVANT toute écriture
        Map<String, StockItemEntity> stockBySku = new HashMap<>();
        Map<String, Integer> requestedBySku = new HashMap<>();
        for (OrderItem item : orderPlacedEvent.getItems()) {
            String sku = item.getSku();

            Optional<StockItemEntity> stockItem = stockItemRepository.findBySku(sku);
            if (stockItem.isEmpty()) {
                log.warn("❌ No stock for SKU={}", sku);
                reject(orderPlacedEvent, item, "Unknown SKU=" + sku);
                return; // ❌ arrêt de la saga
            }
            stockBySku.put(sku, stockItem.get());

            // Une même commande peut contenir plusieurs lignes du même SKU
            int requestedQty = requestedBySku.merge(sku, item.getQuantity(), Integer::sum);
            int availableQty = stockItem.get().getAvailableQuantity();
            if (availableQty < requestedQty) {
                log.warn("Not enough stock for SKU= {}, (requestedQty= {}, available= {})",
                        sku, requestedQty, availableQty);
                reject(orderPlacedEvent, item, "Insufficient stock for SKU=" + sku);
                return; // ❌ arrêt de la saga
            }
        }

        // 2. Tout est disponible : réservation de chaque item
        for (OrderItem item : orderPlacedEvent.getItems()) {
            updateStock(stockBySku.get(item.getSku()), item.getQuantity());
            saveReservation(orderId, item.getSku(), item.getQuantity(), RESERVED);
            log.info("✅ Stock reserved for SKU={} | qty={}", item.getSku(), item.getQuantity());
        }

        publishStockReserved(orderPlacedEvent);
    }

    /**
     * Rejette la commande : trace le rejet (qui sert aussi de marqueur d'idempotence)
     * et écrit un {@code StockRejectedEvent} dans l'outbox. Aucun stock n'est modifié.
     *
     * @param orderPlacedEvent événement de commande à l’origine du rejet
     * @param item item de commande à l’origine du rejet
     * @param reason motif du rejet transmis à orders-service
     */
    private void reject(OrderPlacedEvent orderPlacedEvent, OrderItem item, String reason) {
        saveReservation(UUID.fromString(orderPlacedEvent.getOrderId()), item.getSku(), item.getQuantity(), REJECTED);
        publishStockRejected(orderPlacedEvent, reason);
    }

    /**
     * Met à jour les quantités de stock suite à une réservation.
     * <p>
     * - Diminue la quantité disponible
     * - Augmente la quantité réservée
     * - Met à jour la date de modification
     *
     * @param stockItem entité stock à mettre à jour
     * @param requestedQty quantité à réserver
     */
    private void updateStock(StockItemEntity stockItem, Integer requestedQty) {
        stockItem.setAvailableQuantity(stockItem.getAvailableQuantity() - requestedQty);
        stockItem.setReservedQuantity(stockItem.getReservedQuantity() + requestedQty);
        stockItem.setUpdatedAt(LocalDateTime.now());
        stockItemRepository.save(stockItem);
    }

    /**
     * Enregistre l’historique d’une réservation ou d’un rejet de stock.
     *
     * @param orderId identifiant de la commande
     * @param sku identifiant du produit
     * @param qty quantité concernée
     * @param status statut de la réservation (RESERVED ou REJECTED)
     */
    private void saveReservation(UUID orderId, String sku, Integer qty, StockReservationStatus status) {
        stockReservationRepository.save(
                StockReservationEntity.builder()
                        .orderId(orderId)
                        .sku(sku)
                        .quantity(qty)
                        .status(status)
                        .createdAt(LocalDateTime.now())
                        .build());
    }

    /**
     * Écrit un événement {@code StockRejectedEvent} dans l'outbox afin de signaler
     * l’échec de la réservation de stock pour une commande.
     *
     * @param orderPlacedEvent événement de commande à l’origine du rejet
     * @param reason motif du rejet
     */
    private void publishStockRejected(OrderPlacedEvent orderPlacedEvent, String reason) {
        stockEventPublisher.publishStockRejected(StockRejectedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderPlacedEvent.getOrderId())
                .setIdempotencyKey(orderPlacedEvent.getIdempotencyKey())
                .setReason(reason)
                .setRejectedAt(Instant.now())
                .setVersion("v1")
                .build());
    }

    /**
     * Écrit un événement {@code StockReservedEvent} dans l'outbox lorsque tous les
     * items d'une commande ont été réservés avec succès.
     *
     * @param orderPlacedEvent événement de commande contenant les items réservés
     */
    private void publishStockReserved(OrderPlacedEvent orderPlacedEvent) {
        stockEventPublisher.publishStockReserved(StockReservedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderPlacedEvent.getOrderId())
                .setItems(orderPlacedEvent.getItems().stream()
                        .map(item -> StockItem.newBuilder()
                                .setSku(item.getSku())
                                .setQuantity(item.getQuantity())
                                .build())
                        .toList())
                .setReservedAt(Instant.now())
                .setVersion("v1")
                .build());
        log.info("📡 StockReservedEvent écrit dans l'outbox pour orderId={}", orderPlacedEvent.getOrderId());
    }

}
