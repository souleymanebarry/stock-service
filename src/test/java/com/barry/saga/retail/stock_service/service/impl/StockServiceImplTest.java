package com.barry.saga.retail.stock_service.service.impl;

import com.barry.saga.retail.order.event.OrderItem;
import com.barry.saga.retail.order.event.OrderPlacedEvent;
import com.barry.saga.retail.stock.event.StockRejectedEvent;
import com.barry.saga.retail.stock.event.StockReservedEvent;
import com.barry.saga.retail.stock_service.entities.StockItemEntity;
import com.barry.saga.retail.stock_service.entities.StockReservationEntity;
import com.barry.saga.retail.stock_service.entities.enums.StockReservationStatus;
import com.barry.saga.retail.stock_service.outbox.StockEventPublisher;
import com.barry.saga.retail.stock_service.repositories.StockItemRepository;
import com.barry.saga.retail.stock_service.repositories.StockReservationRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StockServiceImplTest {

    private static final UUID ORDER_ID = UUID.randomUUID();

    @Mock
    private StockItemRepository stockItemRepository;

    @Mock
    private StockReservationRepository stockReservationRepository;

    @Mock
    private StockEventPublisher stockEventPublisher;

    @InjectMocks
    private StockServiceImpl stockService;

    @Test
    @DisplayName("réserve tous les items et publie un StockReservedEvent quand le stock suffit")
    void reservesAllItems_whenStockIsSufficient() {
        StockItemEntity iphone = stock("SKU-IPH15P", 10);
        StockItemEntity airpods = stock("SKU-APP2", 5);
        when(stockItemRepository.findBySku("SKU-IPH15P")).thenReturn(Optional.of(iphone));
        when(stockItemRepository.findBySku("SKU-APP2")).thenReturn(Optional.of(airpods));

        stockService.reservedStock(order(item("SKU-IPH15P", 2), item("SKU-APP2", 5)));

        assertThat(iphone.getAvailableQuantity()).isEqualTo(8);
        assertThat(iphone.getReservedQuantity()).isEqualTo(2);
        assertThat(airpods.getAvailableQuantity()).isZero();
        assertThat(airpods.getReservedQuantity()).isEqualTo(5);
        assertThat(savedReservationStatuses()).containsExactly(StockReservationStatus.RESERVED, StockReservationStatus.RESERVED);

        ArgumentCaptor<StockReservedEvent> captor = ArgumentCaptor.forClass(StockReservedEvent.class);
        verify(stockEventPublisher).publishStockReserved(captor.capture());
        assertThat(captor.getValue().getOrderId()).isEqualTo(ORDER_ID.toString());
        assertThat(captor.getValue().getItems()).hasSize(2);
        verify(stockEventPublisher, never()).publishStockRejected(any());
    }

    @Test
    @DisplayName("tout ou rien : aucun stock n'est réservé si un item suivant est insuffisant")
    void reservesNothing_whenALaterItemIsInsufficient() {
        StockItemEntity iphone = stock("SKU-IPH15P", 10);
        StockItemEntity xps = stock("SKU-XPS15", 2);
        when(stockItemRepository.findBySku("SKU-IPH15P")).thenReturn(Optional.of(iphone));
        when(stockItemRepository.findBySku("SKU-XPS15")).thenReturn(Optional.of(xps));

        stockService.reservedStock(order(item("SKU-IPH15P", 1), item("SKU-XPS15", 3)));

        assertThat(iphone.getAvailableQuantity()).isEqualTo(10);
        assertThat(xps.getAvailableQuantity()).isEqualTo(2);
        verify(stockItemRepository, never()).save(any());
        assertThat(savedReservationStatuses()).containsExactly(StockReservationStatus.REJECTED);
        assertThat(publishedRejection().getReason()).isEqualTo("Insufficient stock for SKU=SKU-XPS15");
        verify(stockEventPublisher, never()).publishStockReserved(any());
    }

    @Test
    @DisplayName("rejette la commande (au lieu de lever une exception) quand le SKU est inconnu")
    void rejects_whenSkuIsUnknown() {
        when(stockItemRepository.findBySku("SKU-UNKNOWN")).thenReturn(Optional.empty());

        stockService.reservedStock(order(item("SKU-UNKNOWN", 1)));

        verify(stockItemRepository, never()).save(any());
        assertThat(savedReservationStatuses()).containsExactly(StockReservationStatus.REJECTED);
        assertThat(publishedRejection().getReason()).isEqualTo("Unknown SKU=SKU-UNKNOWN");
    }

    @Test
    @DisplayName("cumule les quantités de plusieurs lignes du même SKU avant de vérifier le stock")
    void cumulatesQuantities_forSameSku() {
        StockItemEntity ps5 = stock("SKU-PS5SLIM", 5);
        when(stockItemRepository.findBySku("SKU-PS5SLIM")).thenReturn(Optional.of(ps5));

        stockService.reservedStock(order(item("SKU-PS5SLIM", 3), item("SKU-PS5SLIM", 3)));

        assertThat(ps5.getAvailableQuantity()).isEqualTo(5);
        verify(stockItemRepository, never()).save(any());
        assertThat(publishedRejection().getReason()).isEqualTo("Insufficient stock for SKU=SKU-PS5SLIM");
    }

    @Test
    @DisplayName("idempotence : ignore une commande déjà traitée (redélivrance Kafka)")
    void skips_whenOrderAlreadyProcessed() {
        when(stockReservationRepository.existsByOrderId(ORDER_ID)).thenReturn(true);

        stockService.reservedStock(order(item("SKU-IPH15P", 1)));

        verifyNoInteractions(stockItemRepository, stockEventPublisher);
        verify(stockReservationRepository, never()).save(any());
    }

    private List<StockReservationStatus> savedReservationStatuses() {
        ArgumentCaptor<StockReservationEntity> captor = ArgumentCaptor.forClass(StockReservationEntity.class);
        verify(stockReservationRepository, atLeastOnce()).save(captor.capture());
        return captor.getAllValues().stream().map(StockReservationEntity::getStatus).toList();
    }

    private StockRejectedEvent publishedRejection() {
        ArgumentCaptor<StockRejectedEvent> captor = ArgumentCaptor.forClass(StockRejectedEvent.class);
        verify(stockEventPublisher).publishStockRejected(captor.capture());
        assertThat(captor.getValue().getOrderId()).isEqualTo(ORDER_ID.toString());
        return captor.getValue();
    }

    private static StockItemEntity stock(String sku, int available) {
        return StockItemEntity.builder()
                .stockId(UUID.randomUUID())
                .sku(sku)
                .availableQuantity(available)
                .reservedQuantity(0)
                .build();
    }

    private static OrderItem item(String sku, int quantity) {
        return OrderItem.newBuilder()
                .setSku(sku)
                .setQuantity(quantity)
                .setUnitPrice(new BigDecimal("10.00"))
                .build();
    }

    private static OrderPlacedEvent order(OrderItem... items) {
        return OrderPlacedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(ORDER_ID.toString())
                .setCustomerId("cust-1")
                .setItems(List.of(items))
                .setTotalAmount(new BigDecimal("10.00"))
                .setCreatedAt(Instant.now())
                .setIdempotencyKey("idem-1")
                .build();
    }
}
