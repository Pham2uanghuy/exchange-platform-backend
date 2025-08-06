package treiding.hpq.marketdataservice.kafka;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.marketdataservice.store.MarketDataStore;
import treiding.hpq.marketdataservice.websocket.MarketDataWebSocketHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import jakarta.annotation.PostConstruct;
import java.util.Properties;

@Component
public class TradeEventConsumer extends BaseKafkaConsumer<Trade> {

    // Define the correct topic name for trade events
    public static final String TRADE_EVENTS_TOPIC = "trade.events";

    // Inject the service responsible for handling trade events specific to Market Data
    private final MarketDataStore marketDataStore;
    private final MarketDataWebSocketHandler webSocketHandler; // To push updates to WebSocket clients

    /**
     * Constructs the TradeEventConsumer for the Market Data Service.
     * Spring will inject the necessary Kafka properties and the dependent services.
     *
     * @param bootstrapServers The Kafka bootstrap server addresses.
     * @param groupId The consumer group ID for this specific consumer, unique for Market Data Service.
     * @param marketDataStore The store responsible for maintaining market data (last price, trade history).
     * @param webSocketHandler The handler to push real-time updates to connected clients via WebSocket.
     */
    public TradeEventConsumer(
            @Value("${spring.kafka.consumer.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id.trade-events}") String groupId,
            MarketDataStore marketDataStore,
            MarketDataWebSocketHandler webSocketHandler) {

        // Call the superclass constructor with specific properties and topic name
        super(buildConsumerProps(bootstrapServers, groupId), TRADE_EVENTS_TOPIC);
        this.marketDataStore = marketDataStore;
        this.webSocketHandler = webSocketHandler;
    }

    /**
     * Helper method to build Kafka consumer properties specifically for trade events.
     * This ensures correct deserializers and other settings for consuming Trade objects.
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID for this consumer.
     * @return Properties object for Kafka consumer.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());

        // Configure JsonDeserializer for the value (Trade object)
        props.put("value.deserializer", JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Trade.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        return props;
    }

    /**
     * Implements the abstract method from BaseKafkaConsumer to create a concrete KafkaConsumer instance.
     * This method simply uses the 'consumerProps' already built to create the KafkaConsumer.
     *
     * @param consumerProps The base properties provided to the consumer.
     * @param topicName The Kafka topic this consumer will listen to.
     * @return An initialized KafkaConsumer configured to deserialize String keys and Trade JSON values.
     */
    @Override
    protected Consumer<String, Trade> createKafkaConsumer(Properties consumerProps, String topicName) {
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Starts the Kafka consumer thread to listen for trade events.
     * This method is called automatically by Spring after dependency injection is complete.
     */
    @PostConstruct
    public void startListening() {
        log.info("Starting MarketDataService TradeEventConsumer for topic: {}", TRADE_EVENTS_TOPIC);
        startConsuming(TRADE_EVENTS_TOPIC, trade -> {
            log.info("Received trade: {}, instrumentId: {} ", trade.getTradeId(), trade.getInstrumentId());
            marketDataStore.updateMarketData(trade); // Update the in-memory store
            webSocketHandler.notifyTradeUpdate(trade); // Push update to WebSocket clients
        });
    }

    /**
     * Stops the consumer gracefully when the application shuts down.
     */
    @PreDestroy
    public void stopListening() {
        log.info("Stopping MarketDataService TradeEventConsumer.");
        stopConsuming();
    }
}