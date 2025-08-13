package treiding.hpq.matchingservice.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderConfirmedCancellationEvent;


@Component
public class OrderCancellationProducer extends BaseKafkaProducer<Object> {


    private final String orderCancellationConfirmationTopic;

    public OrderCancellationProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topics.order-cancellation-confirmation}") String orderCancellationConfirmationTopic) {
        super(kafkaTemplate);
        this.orderCancellationConfirmationTopic = orderCancellationConfirmationTopic;
    }

    /**
     * Sends an OrderConfirmedCancellationEvent to the designated topic.
     * This indicates that an order has been successfully canceled by the Matching Engine.
     *
     * @param event The OrderConfirmedCancellationEvent to send.
     */
    public void sendConfirmedCancellationEvent(OrderConfirmedCancellationEvent event) {
        sendMessage(orderCancellationConfirmationTopic, event.getOrderId(), event);
        log.info("Sent OrderConfirmedCancellationEvent for order ID: {}", event.getOrderId());
    }

    /**
     * Sends an OrderCancellationRejectedEvent to the designated topic.
     * This indicates that an order cancellation request was rejected by the Matching Engine.
     *
     * @param event The OrderCancellationRejectedEvent to send.
     */
    public void sendRejectedCancellationEvent(OrderCancellationRejectedEvent event) {
        sendMessage(orderCancellationConfirmationTopic, event.getOrderId(), event);
        log.warn("Sent OrderCancellationRejectedEvent for order ID: {}. Reason: {}", event.getOrderId(), event.getReason());
    }

    public String getOrderCancellationConfirmationTopic() {
        return orderCancellationConfirmationTopic;
    }
}
