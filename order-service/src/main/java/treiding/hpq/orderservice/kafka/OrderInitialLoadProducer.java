package treiding.hpq.orderservice.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;

@Component
public class OrderInitialLoadProducer extends BaseKafkaProducer<Order> {

    private static final Logger log = LoggerFactory.getLogger(OrderInitialLoadProducer.class);

    // This topic is for the initial load of existing open orders.
    // The Matching Engine's OrderInitialLoadConsumer will consume from this topic.
    @Value("${kafka.topic.initial-load}")
    private String INITIAL_LOAD_TOPIC;

    /**
     * Constructs the OrderInitialLoadProducer, injecting the KafkaTemplate.
     * Spring will automatically provide a suitable KafkaTemplate<String, Order> bean.
     *
     * @param kafkaTemplate The KafkaTemplate instance used for sending messages.
     */
    public OrderInitialLoadProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        super(kafkaTemplate); // Call the superclass constructor to initialize kafkaTemplate
    }


    /**
     * Get TOPIC of producer
     * @return
     */
    public String getInitialLoadTopic() {
        return INITIAL_LOAD_TOPIC;
    }

    /**
     * Sends an order to the dedicated initial load topic.
     * This is used for syncing existing open/partially filled orders when a new service starts.
     * The message key will be the order's ID.
     *
     * @param order The Order object to send (expected to be an existing open/partially filled order).
     */
    public void sendInitialLoadOrder(Order order) {
        if (order == null || order.getOrderId() == null) {
            log.error("Cannot send initial load order: Order object or its ID is null.");
            return;
        }
        sendMessage(INITIAL_LOAD_TOPIC, order.getOrderId(), order);
        log.debug("Sent initial load order for Order ID: {} to topic {}", order.getOrderId(), INITIAL_LOAD_TOPIC);
    }
}