package treiding.hpq.matchingservice.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;
import treiding.hpq.basedomain.kafkaevent.OrderMatchedEvent;

@Component
public class OrderMatchedEventProducer extends BaseKafkaProducer<OrderMatchedEvent> {

    @Value("${kafka.topic.order-matched-events}")
    private String ORDER_MATCHED_EVENTS_TOPIC;

    /**
     * Constructor for OrderStatusUpdateProducer.
     * Injects the KafkaTemplate configured for sending OrderMatchedEvent objects.
     *
     * @param kafkaTemplate The KafkaTemplate instance.
     */
    public OrderMatchedEventProducer(KafkaTemplate<String, OrderMatchedEvent> kafkaTemplate) {
        super(kafkaTemplate); // Call the superclass constructor to inject KafkaTemplate
    }

    /**
     * Sends an OrderMatchedEvent to Kafka.
     * The key for the Kafka message will be the unique trade ID, ensuring that
     * all events related to the same trade are partitioned together.
     *
     * @param event The OrderMatchedEvent object containing details of the matched orders and trade.
     */
    public void sendOrderStatusUpdate(OrderMatchedEvent event) {
        if (event == null || event.getTradeId() == null) {
            log.error("Cannot send order matched event: Trade ID is null for event {}. Skipping.", event);
            return;
        }
        // Use the tradeId as the message key to ensure all events for a single trade go to the same partition.
        sendMessage(ORDER_MATCHED_EVENTS_TOPIC, event.getTradeId(), event);
        log.info("Sent OrderMatchedEvent to topic '{}' for Trade ID: {}", ORDER_MATCHED_EVENTS_TOPIC, event.getTradeId());
    }


    /**
     * Returns the Kafka topic used for publishing order matched events.
     *
     * @return The topic name.
     */
    public String getOrderMatchedEventsTopic() {
        return ORDER_MATCHED_EVENTS_TOPIC;
    }
}