package treiding.hpq.matchingservice.entity;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;

import java.math.BigDecimal;
import  java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
public class OrderBook {

    private final NavigableMap<BigDecimal, List<Order>> bidLevels;
    private final NavigableMap<BigDecimal, List<Order>> askLevels;

    public OrderBook() {
        this.bidLevels = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        this.askLevels = new ConcurrentSkipListMap<>();
    }

    public void addOrder(Order order) {
        // Choose the appropriate Levels based on the OrderSide (BUY or SELL)
        Map<BigDecimal, List<Order>> targetBook =
                (order.getSide() == OrderSide.BUY) ? bidLevels : askLevels;

        // Add the order to the corresponding price level;
        // If the price level does not exist, create a new synchronized list
        targetBook
                .computeIfAbsent(order.getPrice(), k -> Collections.synchronizedList(new LinkedList<>()))
                .add(order);
    }

    public void removeOrder(Order order) {
        // Choose the appropriate Levels based on the OrderSide (BUY or SELL)
        Map<BigDecimal, List<Order>> targetBook =
                (order.getSide() == OrderSide.BUY) ? bidLevels : askLevels;

        // Get Order List at the price of coming order
        List<Order> ordersAtLevel = targetBook.get(order.getPrice());
        if (ordersAtLevel != null) {
            // remove order if exists
            ordersAtLevel.remove(order);
            if (ordersAtLevel.isEmpty()) {
                // remove the price if no available orders at the price
                targetBook.remove(order.getPrice());
            }
        }
    }

    public Iterator<Map.Entry<BigDecimal, List<Order>>> getBidLevelsIterator() {
        return bidLevels.entrySet().iterator();
    }

    public Iterator<Map.Entry<BigDecimal, List<Order>>> getAskLevelsIterator() {
        return  askLevels.entrySet().iterator();
    }

    public void clear() {
        bidLevels.clear();
        askLevels.clear();
    }
}
