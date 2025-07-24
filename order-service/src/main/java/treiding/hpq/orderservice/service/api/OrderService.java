package treiding.hpq.orderservice.service.api;

import treiding.hpq.basedomain.entity.Order;

import java.util.List;

public interface OrderService {
    public Order createOrder(Order order);
    public List<Order> getOrders();
    public Order getOrderByOrderId(String orderId);
    public void cancelOrder(String orderId);
    public void deleteOrder(String orderId);

}
