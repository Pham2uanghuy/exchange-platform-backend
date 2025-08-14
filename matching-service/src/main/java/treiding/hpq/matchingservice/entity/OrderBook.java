package treiding.hpq.matchingservice.entity;

import treiding.hpq.basedomain.entity.Order;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface OrderBook {
    void addOrder(Order order);
    void updateOrder(Order order);
    void removeOrder(Order order);
    Iterator<Map.Entry<BigDecimal, List<Order>>> getBidLevelsIterator(String instrumentId);
    Iterator<Map.Entry<BigDecimal, List<Order>>> getAskLevelsIterator(String instrumentId);
    Order getOrderDetail(String orderId);
    void clear();
}
