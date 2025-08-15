package treiding.hpq.matchingservice.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPool;
import treiding.hpq.matchingservice.entity.OrderBook;
import treiding.hpq.matchingservice.entity.RedisOrderBook;

import java.util.*;

/**
 * Manages the lifecycle and access to multiple OrderBook instances.
 * Each OrderBook is tied to a specific instrumentId. This class
 * acts as a factory and repository for all active order books.
 */
@Component
public class OrderBookManager {

    private static final Logger log = LoggerFactory.getLogger(OrderBookManager.class);

    private final Map<String, OrderBook> orderBooks;
    private final List<String> instrumentIds;

    public OrderBookManager(JedisPool jedisPool, @Value("${app.orderbook.instrument-ids}") String[] instrumentIdsList) {
        if (instrumentIdsList == null || instrumentIdsList.length == 0) {
            log.error("No instrument IDs configured. Cannot initialize any order books.");
            this.orderBooks = Collections.emptyMap();
            this.instrumentIds = Collections.emptyList();
            return;
        }

        Map<String, OrderBook> tempMap = new HashMap<>();
        List<String> tempIdList = new ArrayList<>();

        for (String instrumentId : instrumentIdsList) {
            OrderBook orderBook = new RedisOrderBook(jedisPool, instrumentId);
            tempMap.put(instrumentId, orderBook);
            tempIdList.add(instrumentId);
            log.info("Initialized RedisOrderBook for instrument: {}", instrumentId);
        }

        this.orderBooks = Collections.unmodifiableMap(tempMap);
        this.instrumentIds = Collections.unmodifiableList(tempIdList);
    }

    public OrderBook getOrderBook(String instrumentId) {
        OrderBook orderBook = orderBooks.get(instrumentId);
        if (orderBook == null) {
            log.warn("Attempted to access OrderBook for an unconfigured instrument: {}", instrumentId);
        }
        return orderBook;
    }

    public Map<String, OrderBook> getAllOrderBooks() {
        return orderBooks;
    }

    public List<String> getInstrumentIds() {
        return instrumentIds;
    }
}