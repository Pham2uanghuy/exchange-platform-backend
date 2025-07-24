package treiding.hpq.orderservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;

@Component
public class OrderCommandProducer extends BaseKafkaProducer<Order> {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandProducer.class);

    // This topic is for real-time order commands/events
    // The Matching Engine's OrderEventConsumer will listen to this.
    public static final String ORDER_COMMANDS_TOPIC = "order.events";

    /**
     * Constructs the OrderCommandProducer, injecting the KafkaTemplate.
     * Spring will automatically provide a suitable KafkaTemplate<String, Order> bean.
     *
     * @param kafkaTemplate The KafkaTemplate instance used for sending messages.
     */
    public OrderCommandProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        super(kafkaTemplate); // Call the superclass constructor to initialize kafkaTemplate
    }

    /**
     * Sends an order command or event (e.g., a new order, an order cancellation) to Kafka.
     * The message key will be the order's ID.
     *
     * @param order The Order object representing the command or event.
     */
    public void sendOrderCommand(Order order) {
        if (order == null || order.getOrderId() == null) {
            log.error("Cannot send order command: Order object or its ID is null.");
            return;
        }
        sendMessage(ORDER_COMMANDS_TOPIC, order.getOrderId(), order);
        log.debug("Sent order command/event for Order ID: {} to topic {}", order.getOrderId(), ORDER_COMMANDS_TOPIC);
    }
}