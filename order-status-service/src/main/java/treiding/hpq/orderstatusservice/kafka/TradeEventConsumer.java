package treiding.hpq.orderstatusservice.kafka;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.orderstatusservice.service.OrderStatusUpdateService;

import java.util.Properties;


@Component
public class TradeEventConsumer extends BaseKafkaConsumer<Trade> {

    // Define the correct topic name for trade events
    // This should match the topic defined in your TradeEventProducer
    public static final String TRADE_EVENTS_TOPIC = "trade.events"; // Correct topic name

    // Inject the service responsible for handling trade events
    private final OrderStatusUpdateService orderStatusUpdateService;
    // Alternatively, if you have a dedicated TradeService:
    // private final TradeService tradeService;


    /**
     * Constructs the TradeEventConsumer.
     * Spring will inject the necessary Kafka properties and the service.
     *
     * @param bootstrapServers The Kafka bootstrap server addresses (from application.properties).
     * @param groupId The consumer group ID for this specific consumer (from application.properties).
     * @param orderStatusUpdateService The service responsible for processing trade events.
     * (Or TradeService if you have a dedicated one)
     */
    public TradeEventConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${spring.kafka.consumer.group-id.trade-events}") String groupId, // Correct group ID property
                              OrderStatusUpdateService orderStatusUpdateService) {
        // Call the superclass constructor, passing the built properties and the correct topic name
        super(buildConsumerProps(bootstrapServers, groupId), TRADE_EVENTS_TOPIC);
        this.orderStatusUpdateService = orderStatusUpdateService; // Initialize the injected service
    }

    /**
     * Helper method to build Kafka consumer properties specifically for trade events.
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
        // IMPORTANT: Replace "com.yourcompany.trade" with the actual package of your Trade class!
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Trade.class.getName());

        // For real-time events, 'latest' is often preferred to start consuming new messages.
        props.put("auto.offset.reset", "earliest");
        // For critical data like trades, it's safer to disable auto-commit and commit manually after processing.
        props.put("enable.auto.commit", "false"); // Safer for transactional processing
        return props;
    }

    /**
     * Implements the abstract method from BaseKafkaConsumer to create a concrete KafkaConsumer instance.
     *
     * @param consumerProps The base properties provided to the consumer.
     * @param topicName The Kafka topic this consumer will listen to.
     * @return An initialized KafkaConsumer configured to deserialize String keys and Trade JSON values.
     */
    @Override
    protected Consumer<String, Trade> createKafkaConsumer(Properties consumerProps, String topicName) {
        // This method simply uses the 'consumerProps' already built to create the KafkaConsumer.
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Starts the Kafka consumer thread to listen for trade events.
     * This method is called automatically by Spring after dependency injection is complete.
     */
    @PostConstruct
    public void startListening() {
        log.info("Starting TradeEventConsumer for topic: {}", TRADE_EVENTS_TOPIC);
        // Pass the correct topic name and the method reference to the service's handler for trades
        startConsuming(TRADE_EVENTS_TOPIC, orderStatusUpdateService::handleTradeEvent);
        // If you had a dedicated TradeService: tradeService.startConsuming(TRADE_EVENTS_TOPIC, tradeService::handleTradeEvent);
    }
}