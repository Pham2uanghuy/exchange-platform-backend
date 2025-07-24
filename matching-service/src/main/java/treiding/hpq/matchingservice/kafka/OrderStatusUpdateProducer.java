package treiding.hpq.matchingservice.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;

@Component
public class OrderStatusUpdateProducer extends BaseKafkaProducer<Order> {

    @Value("${kafka.topic.order-status-update}")
    private String ORDER_STATUS_UPDATE_TOPIC;

    public OrderStatusUpdateProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        super(kafkaTemplate); // Call the superclass constructor to inject KafkaTemplate
    }

    /**
     * Sends an order status update event to Kafka.
     * The key for the message will be the order's ID.
     *
     * @param order The Order object with its updated status.
     */
    public void sendOrderStatusUpdate(Order order) {
        if (order.getOrderId() == null) {
            log.error("Cannot send order status update: Order ID is null for order {}", order);
            return;
        }
        sendMessage(ORDER_STATUS_UPDATE_TOPIC, order.getOrderId(), order);
    }
}