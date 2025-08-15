package treiding.hpq.orderstatusservice.kafka;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.orderstatusservice.service.OrderStatusUpdateService;

import java.util.Properties;


/**
 * TradeEventConsumer is a Kafka consumer responsible for reading trade events from the "trade.events" topic.
 *
 * This consumer:
 * - Uses {@link Trade} as the message value type, deserialized from JSON.
 * - Commits offsets manually only after successful processing.
 * - Delegates processing of trade events to {@link OrderStatusUpdateService}.
 * - Inherits common consuming logic from {@link BaseKafkaConsumer}.
 */
@Component
public class TradeEventConsumer extends BaseKafkaConsumer<Trade> {

    /** The Kafka topic name for trade events. */
    public static final String TRADE_EVENTS_TOPIC = "trade.events";

    /** Service responsible for handling trade events. */
    private final OrderStatusUpdateService orderStatusUpdateService;

    /**
     * Constructs a TradeEventConsumer with the required Kafka configuration and processing service.
     * This constructor enables manual offset committing per message for reliability.
     *
     * @param bootstrapServers Kafka bootstrap server addresses (typically from application.properties).
     * @param groupId The Kafka consumer group ID for this consumer instance.
     * @param orderStatusUpdateService Service used to process incoming trade events.
     */
    public TradeEventConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${spring.kafka.consumer.group-id.trade-events}") String groupId,
                              OrderStatusUpdateService orderStatusUpdateService) {
        // commitPerMessage = true, commitBatchSize = 1 for safest processing
        super(buildConsumerProps(bootstrapServers, groupId), TRADE_EVENTS_TOPIC, true, 1);
        this.orderStatusUpdateService = orderStatusUpdateService;
    }

    /**
     * Builds the Kafka consumer properties for this consumer.
     * Configures String key deserialization and JSON value deserialization for {@link Trade} objects.
     * Manual commit is enabled for processing safety.
     *
     * @param bootstrapServers Kafka bootstrap server addresses.
     * @param groupId The Kafka consumer group ID.
     * @return Kafka consumer configuration properties.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Trade.class.getName());

        // Disable auto commit to commit manually after successful processing
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    /**
     * Creates the KafkaConsumer instance for this consumer.
     * Uses String keys and Trade values with JSON deserialization.
     *
     * @param consumerProps Kafka consumer configuration.
     * @param topicName The Kafka topic name (unused here as topic subscription happens in startConsuming()).
     * @return A configured KafkaConsumer.
     */
    @Override
    protected Consumer<String, Trade> createKafkaConsumer(Properties consumerProps, String topicName) {
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Starts consuming trade events from Kafka after Spring has completed dependency injection.
     * Delegates each message to {@link OrderStatusUpdateService#handleTradeEvent(Trade)}.
     */
    @PostConstruct
    public void startListening() {
        log.info("Starting TradeEventConsumer for topic: {}", TRADE_EVENTS_TOPIC);
        startConsuming(TRADE_EVENTS_TOPIC, orderStatusUpdateService::handleTradeEvent);
    }
}
