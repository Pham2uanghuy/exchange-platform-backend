package treiding.hpq.marketdataservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.marketdataservice.store.MarketDataStore;

import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/market-data")
public class MarketDataController {

    private final MarketDataStore marketDataStore;

    public MarketDataController(MarketDataStore marketDataStore) {
        this.marketDataStore = marketDataStore;
    }

    /**
     * REST API to retrieve the last traded price for a given instrument.
     * Example: GET /api/market-data/last-price/BTCUSDT
     * @param instrumentId The ID of the instrument (e.g., "BTCUSDT").
     * @return ResponseEntity containing the last price or 404 if not found.
     */
    @GetMapping("/last-price/{instrumentId}")
    public ResponseEntity<Map<String, BigDecimal>> getLastTradedPrice(@PathVariable String instrumentId) {
        BigDecimal price = marketDataStore.getLastTradedPrice(instrumentId);
        if (price != null) {
            return ResponseEntity.ok(Collections.singletonMap("lastPrice", price));
        }
        return ResponseEntity.notFound().build();
    }

    /**
     * REST API to retrieve recent trade history for a given instrument.
     * Example: GET /api/v1/market-data/trades/BTCUSDT?limit=20
     * @param instrumentId The ID of the instrument.
     * @param limit The maximum number of trades to return (default to 10).
     * @return ResponseEntity containing a list of Trade objects.
     */
    @GetMapping("/trades/{instrumentId}")
    public ResponseEntity<List<Trade>> getTradeHistory(
            @PathVariable String instrumentId,
            @RequestParam(defaultValue = "10") int limit) {
        List<Trade> trades = marketDataStore.getTradeHistory(instrumentId, limit);
        return ResponseEntity.ok(trades);
    }

    /**
     * REST API to retrieve an Order Book snapshot for a given instrument.
     * IMPORTANT: The actual Order Book data typically resides in the Matching Engine.
     * For this demo, you would either:
     * 1. Have the Matching Engine periodically publish full Order Book snapshots to Kafka
     * (then MarketDataStore would maintain it).
     * 2. Call an internal REST API on the Matching Engine from here to get the snapshot.
     * 3. For simplest demo, return dummy data.
     *
     * Example: GET /api/v1/market-data/order-book/BTCUSDT
     * @param instrumentId The ID of the instrument.
     * @return ResponseEntity containing the Order Book snapshot (bids and asks).
     */
    @GetMapping("/order-book/{instrumentId}")
    public ResponseEntity<Map<String, Object>> getOrderBookSnapshot(@PathVariable String instrumentId) {
        // This method relies on how you integrate the Order Book source.
        Map<String, Object> orderBookData = marketDataStore.getOrderBookSnapshot(instrumentId);
        if (orderBookData != null && !orderBookData.isEmpty()) {
            return ResponseEntity.ok(orderBookData);
        }
        return ResponseEntity.notFound().build(); // Or return an empty structure if that's desired for no data
    }
}