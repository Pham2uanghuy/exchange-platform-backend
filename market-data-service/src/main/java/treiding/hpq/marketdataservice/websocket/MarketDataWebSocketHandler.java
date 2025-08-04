package treiding.hpq.marketdataservice.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.marketdataservice.model.MarketDataUpdate;
import treiding.hpq.marketdataservice.model.UpdateType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Component
public class MarketDataWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(MarketDataWebSocketHandler.class);

    // SimpMessagingTemplate is used to send messages to specific destinations,
    // which are then handled by the WebSocket message broker (STOMP).
    private final SimpMessagingTemplate messagingTemplate;

    public MarketDataWebSocketHandler(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    /**
     * Notifies WebSocket clients about a new trade event.
     * Clients subscribed to `/topic/trades/{instrumentId}` will receive this update.
     * @param trade The new Trade object.
     */
    public void notifyTradeUpdate(Trade trade) {
        String destination = "/topic/trades/" + trade.getInstrumentId();
        MarketDataUpdate update = MarketDataUpdate.tradeUpdate(trade);
        messagingTemplate.convertAndSend(destination, update);
        log.debug("Sent trade update via WebSocket to {}: {}", destination, trade.getTradeId());
    }

    /**
     * Notifies WebSocket clients about an updated last traded price.
     * Clients subscribed to `/topic/last-price/{instrumentId}` will receive this update.
     * (You would call this from your MarketDataStore if the price is updated).
     * @param instrumentId The ID of the instrument.
     * @param lastPrice The updated last traded price.
     */
    public void notifyLastPriceUpdate(String instrumentId, BigDecimal lastPrice) {
        String destination = "/topic/last-price/" + instrumentId;
        MarketDataUpdate update = MarketDataUpdate.lastPriceUpdate(instrumentId, lastPrice);
        messagingTemplate.convertAndSend(destination, update);
        log.debug("Sent last price update via WebSocket to {}: {}", destination, lastPrice);
    }

    /**
     * Notifies WebSocket clients about Order Book updates.
     * Clients subscribed to `/topic/order-book/{instrumentId}` will receive this update.
     * This method would be called by the MarketDataStore if it maintains and updates the Order Book.
     * @param instrumentId The ID of the instrument.
     * @param orderBookData A map containing bids and asks.
     */
    public void notifyOrderBookUpdate(String instrumentId, Map<String, Object> orderBookData) {
        String destination = "/topic/order-book/" + instrumentId;
        // Assuming OrderBookData can be directly put into the MarketDataUpdate.data field
        // You might need a specific MarketDataUpdate type for Order Book.
        MarketDataUpdate update = new MarketDataUpdate(
                UpdateType.ORDER_BOOK_UPDATE,
                instrumentId,
                orderBookData,
                Instant.ofEpochMilli(System.currentTimeMillis())
        );
        messagingTemplate.convertAndSend(destination, update);
        log.debug("Sent Order Book update via WebSocket to {}: {}", destination, instrumentId);
    }
}