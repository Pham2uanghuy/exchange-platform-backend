package treiding.hpq.matchingservice.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;

import java.util.Properties;

@Component
public class OrderCommandConsumer extends BaseKafkaConsumer<Order> {

    private static final String ORDER_EVENTS_TOPIC = "order.command";

    /**
     * Constructs the OrderEventConsumer.
     * Spring will inject the necessary Kafka bootstrap servers and group ID from your application.properties.
     *
     * @param bootstrapServers The Kafka bootstrap server addresses (e.g., "localhost:9092").
     * @param groupId The consumer group ID for this specific consumer.
     */
    public OrderCommandConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${spring.kafka.consumer.group-id.order-command}") String groupId) {
        super(buildConsumerProps(bootstrapServers, groupId), ORDER_EVENTS_TOPIC);
    }

    /**
     * Helper method to build Kafka consumer properties specifically for real-time order events.
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
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Order.class.getName());

        // For real-time events, 'latest' is often preferred to start consuming new messages.
        props.put("auto.offset.reset", "latest");
        // For real-time processing, auto-commit can be enabled or manually managed.
        props.put("enable.auto.commit", "true");
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
        return new KafkaConsumer<>(consumerProps);
    }
}