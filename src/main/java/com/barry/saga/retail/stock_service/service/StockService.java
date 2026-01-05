package com.barry.saga.retail.stock_service.service;

import com.barry.saga.retail.stock_service.share.event.OrderPlacedEvent;

public interface StockService {

    void reservedStock(OrderPlacedEvent orderPlacedEvent);
}
