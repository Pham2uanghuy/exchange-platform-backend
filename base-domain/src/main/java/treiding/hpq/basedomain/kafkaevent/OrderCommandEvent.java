package treiding.hpq.basedomain.kafkaevent;

import treiding.hpq.basedomain.entity.Order;

public class OrderCommandEvent {
    private String eventType;
    private Order order;

    public OrderCommandEvent() {
    }

    public OrderCommandEvent(String eventType, Order order) {
        this.eventType = eventType;
        this.order = order;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }
}
