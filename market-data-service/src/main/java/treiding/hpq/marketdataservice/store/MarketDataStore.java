package treiding.hpq.marketdataservice.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Trade;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Component
public class MarketDataStore {

    private static final Logger log = LoggerFactory.getLogger(MarketDataStore.class);

    // Stores the last traded price for each instrument.
    private final ConcurrentHashMap<String, BigDecimal> lastTradedPrices = new ConcurrentHashMap<>();

    // Stores the recent trade history for each instrument.
    // CopyOnWriteArrayList is used for thread-safety during reads and writes,
    // though for very high-frequency updates, a more specialized concurrent queue might be considered.
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Trade>> tradeHistories = new ConcurrentHashMap<>();

    // Maximum number of trade history entries to keep for each instrument.
    private static final int MAX_TRADE_HISTORY_SIZE = 100;

    /**
     * Updates the market data when a new trade occurs.
     * This method is called by the Kafka consumer.
     * @param trade The Trade object received from the Matching Engine.
     */
    public void updateMarketData(Trade trade) {
        String instrumentId = trade.getInstrumentId();
        BigDecimal tradePrice = trade.getTradedPrice();

        // 1. Update the last traded price.
        lastTradedPrices.put(instrumentId, tradePrice);
        log.debug("[MarketDataStore] Updated last traded price for {}: {}", instrumentId, tradePrice);

        // 2. Add to the trade history.
        // Get or create the list for this instrument.
        CopyOnWriteArrayList<Trade> history = tradeHistories.computeIfAbsent(instrumentId, k -> new CopyOnWriteArrayList<>());

        // Add the new trade to the beginning of the list to maintain chronological order (most recent first).
        history.add(0, trade);

        // Keep the history size within the defined limit.
        if (history.size() > MAX_TRADE_HISTORY_SIZE) {
            history.remove(history.size() - 1); // Remove the oldest trade.
        }
        log.debug("[MarketDataStore] Added trade {} to history for {}. Current history size: {}", trade.getTradeId(), instrumentId, history.size());
    }

    /**
     * Retrieves the last traded price for a given instrument.
     * @param instrumentId The ID of the instrument.
     * @return The last traded price, or null if no trades have occurred for this instrument.
     */
    public BigDecimal getLastTradedPrice(String instrumentId) {
        return lastTradedPrices.get(instrumentId);
    }

    /**
     * Retrieves the recent trade history for a given instrument.
     * @param instrumentId The ID of the instrument.
     * @param limit The maximum number of trades to retrieve.
     * @return A list of recent trades, sorted by timestamp in descending order.
     */
    public List<Trade> getTradeHistory(String instrumentId, int limit) {
        CopyOnWriteArrayList<Trade> history = tradeHistories.getOrDefault(instrumentId, new CopyOnWriteArrayList<>());
        return history.stream()
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Placeholder for Order Book retrieval.
     * In this basic demo, Market Data Service doesn't maintain the full Order Book itself.
     * It would either:
     * A) Call an API on the Matching Engine to get a snapshot.
     * B) Listen to dedicated "Order Book Snapshot/Delta" Kafka topics from the Matching Engine.
     */
    public Map<String, Object> getOrderBookSnapshot(String instrumentId) {
        return Collections.emptyMap(); // Return empty map if no real data source is integrated yet
    }
}