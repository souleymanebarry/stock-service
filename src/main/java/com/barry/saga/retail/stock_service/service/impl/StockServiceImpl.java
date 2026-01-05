package com.barry.saga.retail.stock_service.service.impl;

import com.barry.saga.retail.stock_service.entities.StockItemEntity;
import com.barry.saga.retail.stock_service.entities.StockReservationEntity;
import com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus;
import com.barry.saga.retail.stock_service.kafka.StockEventProducer;
import com.barry.saga.retail.stock_service.repositories.StockItemRepository;
import com.barry.saga.retail.stock_service.repositories.StockReservationRepository;
import com.barry.saga.retail.stock_service.service.StockService;
import com.barry.saga.retail.stock_service.share.event.OrderPlacedEvent;

import com.barry.saga.retail.stock_service.share.event.StockRejectedEvent;
import com.barry.saga.retail.stock_service.share.event.StockReservedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

import static com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus.REJECTED;
import static com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus.RESERVED;

@Service
@RequiredArgsConstructor
@Log4j2
public class StockServiceImpl implements StockService {

    private final StockItemRepository stockItemRepository;
    private final StockReservationRepository stockReservationRepository;
    private final StockEventProducer stockEventProducer;

    /**
     * Traite un événement {@link OrderPlacedEvent} reçu depuis Kafka.
     * <p>
     * Cette méthode implémente la logique de réservation de stock dans le cadre
     * d’une saga :
     * <ul>
     *     <li>Vérifie la disponibilité du stock pour chaque item</li>
     *     <li>Réserve les quantités si le stock est suffisant</li>
     *     <li>Arrête la saga dès qu’un item ne peut pas être réservé</li>
     *     <li>Publie un événement {@code StockReservedEvent} ou {@code StockRejectedEvent}</li>
     * </ul>
     *
     * L’ensemble du traitement est transactionnel :
     * soit toutes les réservations réussissent, soit aucune n’est validée.
     *
     * @param orderPlacedEvent événement de commande contenant les items à réserver
     */
    @Override
    @Transactional
    public void reservedStock(OrderPlacedEvent orderPlacedEvent) {
        for (OrderPlacedEvent.Item item : orderPlacedEvent.getItems()) {
            if (!reserveItem(orderPlacedEvent.getOrderId(), item)) {
                publishStockRejected(orderPlacedEvent.getOrderId(), item.getSku());
                return; //  // ❌ arrêt de la saga
            }
        }

        publishStockReserved(orderPlacedEvent);
    }

    /**
     * Tente de réserver le stock pour un item de commande donné.
     * <p>
     * Étapes :
     * <ul>
     *     <li>Récupération du stock pour le SKU</li>
     *     <li>Vérification de la quantité disponible</li>
     *     <li>Mise à jour des quantités disponibles et réservées</li>
     *     <li>Enregistrement de l’historique de réservation</li>
     * </ul>
     *
     * @param orderId identifiant de la commande
     * @param item item de commande à réserver
     * @return {@code true} si la réservation a réussi, {@code false} sinon
     */
    private boolean reserveItem(UUID orderId, OrderPlacedEvent.Item item) {
        String sku = item.getSku();
        Integer requestedQty = item.getQuantity();

        final StockItemEntity stockItem = findStockItem(sku);

        if (stockItem.getAvailableQuantity() < requestedQty) {
            log.warn("Not enough stock for SKU= {}, (requestedQty= {}, available= {})",
                    sku, requestedQty, stockItem.getAvailableQuantity());
            saveReservation(orderId,sku,requestedQty,REJECTED);
            return false;
        }
        updateStock(stockItem,requestedQty);
        log.info("✅ Stock reserved for SKU={} | qty={}", sku, requestedQty);
        saveReservation(orderId, sku, requestedQty, RESERVED);
        return true;
    }

    private StockItemEntity findStockItem(String sku) {
        return stockItemRepository.findBySku(sku)
                .orElseThrow(() -> {
                    log.error("❌ No stock for SKU={}", sku);
                    return new IllegalArgumentException("Stock not found for SKU={}" + sku);
                });
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
     * Publie un événement {@code StockRejectedEvent} afin de signaler
     * l’échec de la réservation de stock pour une commande.
     *
     * @param orderId identifiant de la commande
     * @param sku identifiant du produit à l’origine du rejet
     */
    private void publishStockRejected(UUID orderId, String sku) {
        stockEventProducer.sendStockRejected(StockRejectedEvent.builder()
                .orderId(orderId)
                .reason("Insufficient stock for Sku= {}" + sku)
                .rejectedAt(LocalDateTime.now())
                .build());
    }

    /**
     * Publie un "évènement {@code StockReservedEvent } lorsque tous les items d'une
     * cmd ont été réservés avec succès
     *
     * @param orderPlacedEvent  évènement de commande mes contenant les items réservés
     */
    private void  publishStockReserved(OrderPlacedEvent orderPlacedEvent) {
        stockEventProducer.sendStockReserved(StockReservedEvent.builder()
                .orderId(orderPlacedEvent.getOrderId())
                        .reservedAt(LocalDateTime.now())
                        .items(orderPlacedEvent.getItems().stream()
                                .map(item -> StockReservedEvent.StockItem.builder()
                                        .sku(item.getSku())
                                        .quantity(item.getQuantity())
                                        .build()).toList())
                .build());
        log.info("📡 StockReservedEvent publié pour orderId={}", orderPlacedEvent.getOrderId());
    }

}
