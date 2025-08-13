package treiding.hpq.orderstatusservice.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderConfirmedCancellationEvent;
import treiding.hpq.orderstatusservice.service.OrderStatusUpdateService;

import java.util.Properties;


@Component
public class OrderCancellationConsumer extends BaseKafkaConsumer<Object> { // Use Object to handle both event types

    private static final String confirmationTopic = "order-cancellation-events";
    private final OrderStatusUpdateService orderStatusUpdateService; // Inject OrderStatusUpdateService

    public OrderCancellationConsumer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id.order-cancellation-events}") String groupId,
            OrderStatusUpdateService orderStatusUpdateService) { // Inject the service that will handle updates

        super(createConsumerProps(bootstrapServers, groupId), confirmationTopic);
        this.orderStatusUpdateService = orderStatusUpdateService;
        log.info("OrderCancellationEventListener initialized for topic: {}", confirmationTopic);
    }

    // Static helper method to create consumer properties
    private static Properties createConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", OrderCancellationEventDeserializer.class.getName()); // Use our custom deserializer
        props.put("auto.offset.reset", "earliest"); // Start reading from the beginning if no offset is found
        props.put("enable.auto.commit", "true"); // Auto commit offsets for simplicity in this example
        props.put("auto.commit.interval.ms", "1000"); // Commit every second
        return props;
    }

    @Override
    protected Consumer<String, Object> createKafkaConsumer(Properties consumerProps, String topicName) {
        // This method from BaseKafkaConsumer is called during construction
        return new KafkaConsumer<>(consumerProps);
    }

    @PostConstruct
    public void start() {
        log.info("Starting OrderCancellationEventListener consumer thread.");
        startConsuming(confirmationTopic, this::processCancellationEvent);
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping OrderCancellationEventListener consumer thread.");
        stopConsuming();
    }

    /**
     * Internal method to process different types of cancellation events.
     * This is the eventHandler lambda passed to BaseKafkaConsumer.
     * @param event The deserialized event object (either OrderConfirmedCancellationEvent or OrderCancellationRejectedEvent).
     */
    private void processCancellationEvent(Object event) {
        if (event instanceof OrderConfirmedCancellationEvent) {
            orderStatusUpdateService.handleOrderConfirmedCancelledEvent((OrderConfirmedCancellationEvent) event);
        } else if (event instanceof OrderCancellationRejectedEvent) {
            orderStatusUpdateService.handleOrderCancellationRejectedEvent((OrderCancellationRejectedEvent) event);
        } else {
            log.error("Received unknown event type for cancellation confirmation topic: {}", event.getClass().getName());
        }
    }
}
