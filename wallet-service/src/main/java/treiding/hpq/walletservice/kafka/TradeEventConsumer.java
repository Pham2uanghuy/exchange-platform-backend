package treiding.hpq.walletservice.kafka;



import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;

import java.util.Properties;

@Component
public class TradeEventConsumer extends BaseKafkaConsumer<Trade> {

    public static final String TRADE_EVENTS_TOPIC = "trade.events";

    // Inject the service that contains the core business logic for processing trade events.
    private final TradeEventProcessor tradeEventProcessor;

    /**
     * Constructs the TradeEventConsumer.
     * Spring will inject the necessary Kafka properties and the dependent service.
     *
     * @param bootstrapServers The Kafka bootstrap server addresses.
     * @param groupId The consumer group ID for this consumer.
     * @param tradeEventProcessor The service responsible for handling trade events.
     */
    public TradeEventConsumer(
            @Value("${spring.kafka.consumer.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            TradeEventProcessor tradeEventProcessor) {

        // Pass the built properties and the topic name to the superclass constructor.
        super(buildConsumerProps(bootstrapServers, groupId), TRADE_EVENTS_TOPIC);
        this.tradeEventProcessor = tradeEventProcessor;
    }

    /**
     * Helper method to build Kafka consumer properties for trade events.
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID.
     * @return Properties object for Kafka consumer.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());

        // Configure JsonDeserializer to trust all packages and specify the target class.
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Trade.class.getName());

        // For critical data, it's often best to handle offsets manually after processing.
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        return props;
    }

    /**
     * Implements the abstract method from BaseKafkaConsumer to create a concrete KafkaConsumer.
     * This method simply instantiates KafkaConsumer with the provided properties.
     *
     * @param consumerProps The properties for the Kafka consumer.
     * @param topicName The topic to consume from.
     * @return A new KafkaConsumer instance.
     */
    @Override
    protected Consumer<String, Trade> createKafkaConsumer(Properties consumerProps, String topicName) {
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Lifecycle method to start the consumer thread automatically after dependency injection.
     */
    @PostConstruct
    public void startListening() {
        log.info("Starting TradeEventConsumer for topic: {}", TRADE_EVENTS_TOPIC);
        startConsuming(TRADE_EVENTS_TOPIC, tradeEventProcessor::handleTradeEvent);
    }

    /**
     * Lifecycle method to stop the consumer gracefully before the application shuts down.
     */
    @PreDestroy
    public void stopListening() {
        log.info("Stopping TradeEventConsumer.");
        stopConsuming();
    }
}
