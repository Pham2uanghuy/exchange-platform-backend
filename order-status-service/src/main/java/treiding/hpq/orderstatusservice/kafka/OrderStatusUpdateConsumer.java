package treiding.hpq.orderstatusservice.kafka;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.orderstatusservice.service.OrderStatusUpdateService;

import java.util.Properties;

@Component
public class OrderStatusUpdateConsumer extends BaseKafkaConsumer<Order> {

    // Define the specific Kafka topic this consumer listens to
    // This should match the topic defined in your OrderStatusUpdateProducer
    private static final String TOPIC = "order.status.updates"; // Or OrderStatusUpdateProducer.ORDER_STATUS_UPDATE_TOPIC;

    private final OrderStatusUpdateService orderStatusUpdateService; // Inject the service to handle updates

    /**
     * Constructs the OrderStatusUpdateConsumer.
     * Spring will inject the necessary Kafka properties and the service.
     *
     * @param bootstrapServers The Kafka bootstrap server addresses (from application.properties).
     * @param groupId The consumer group ID for this specific consumer (from application.properties).
     * @param orderStatusUpdateService The service responsible for processing order status updates.
     */
    public OrderStatusUpdateConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                     @Value("${spring.kafka.consumer.group-id.order-status-updates}") String groupId,
                                     OrderStatusUpdateService orderStatusUpdateService) { // Corrected parameter name & injection
        // Call the superclass constructor, passing the built properties and the fixed topic name
        super(buildConsumerProps(bootstrapServers, groupId), TOPIC);
        this.orderStatusUpdateService = orderStatusUpdateService; // Initialize the injected service
    }

    /**
     * Helper method to build Kafka consumer properties specifically for order status updates.
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID for this consumer.
     * @return Properties object for Kafka consumer.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());

        // Configure JsonDeserializer for the value
        props.put("value.deserializer", JsonDeserializer.class.getName());
        // IMPORTANT: Replace "*" with your actual trusted packages for security reasons!
        // For example: props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.yourcompany.order");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); // **SECURITY WARNING: DO NOT USE * IN PRODUCTION!**
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Order.class.getName());

        // For real-time events, 'latest' is often preferred to start consuming new messages.
        props.put("auto.offset.reset", "latest");
        // For real-time processing, auto-commit can be enabled or manually managed.
        // It's often recommended to disable auto-commit and commit manually after processing.
        props.put("enable.auto.commit", "false"); // Setting to false for more control over commit
        return props;
    }

    /**
     * Implements the abstract method from BaseKafkaConsumer to create a concrete KafkaConsumer instance.
     *
     * @param consumerProps The base properties provided to the consumer.
     * @param topicName The Kafka topic this consumer will listen to.
     * @return An initialized KafkaConsumer configured to deserialize String keys and Order JSON values.
     */
    @Override
    protected Consumer<String, Order> createKafkaConsumer(Properties consumerProps, String topicName) {
        // This method simply uses the 'consumerProps' already built to create the KafkaConsumer.
        // The topicName parameter is already handled by BaseKafkaConsumer for subscription.
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Starts the Kafka consumer thread to listen for order status updates.
     * This method is called automatically by Spring after dependency injection is complete.
     */
    @PostConstruct
    public void startListening() {
        log.info("Starting OrderStatusUpdateConsumer for topic: {}", TOPIC);
        // Pass the fixed topic name and the method reference to the service's handler
        startConsuming(TOPIC, orderStatusUpdateService::handleOrderStatusUpdate);
    }
}