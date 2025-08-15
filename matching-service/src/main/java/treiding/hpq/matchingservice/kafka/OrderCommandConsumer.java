package treiding.hpq.matchingservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCommandEvent;

import java.util.Properties;

@Component
public class OrderCommandConsumer extends BaseKafkaConsumer<OrderCommandEvent> {

    private static final String ORDER_EVENTS_TOPIC = "order.command";

    // Shared ObjectMapper instance for better performance and Java 8 time type support
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Constructs an OrderCommandConsumer instance.
     * This consumer subscribes to the "order.command" topic and processes OrderCommandEvent messages.
     * It is configured to manually commit offsets after each message is successfully processed.
     *
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId          Kafka consumer group ID for the order command consumer.
     */
    public OrderCommandConsumer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id.order-command}") String groupId) {
        // commitPerMessage = true, commitBatchSize = 1 (ensures message is committed only when successfully processed)
        super(buildConsumerProps(bootstrapServers, groupId), ORDER_EVENTS_TOPIC, true, 1);
    }

    /**
     * Builds the Kafka consumer configuration properties for the OrderCommandConsumer.
     * This configuration:
     * - Uses a String key deserializer and a JSON value deserializer.
     * - Registers trusted packages for deserialization to avoid security issues.
     * - Disables auto-commit to allow manual offset commit after message processing.
     * - Starts consuming from the earliest offset if no committed offset exists for the group.
     *
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId          Kafka consumer group ID.
     * @return Properties object containing Kafka consumer settings.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());

        // Trusted packages for safe deserialization
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "treiding.hpq.basedomain.kafkaevent,treiding.hpq.basedomain.order");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCommandEvent.class.getName());

        // Disable auto-commit to ensure messages are committed only after successful processing
        props.put("enable.auto.commit", "false");

        // Start from the earliest offset if no committed offset exists
        props.put("auto.offset.reset", "earliest");

        return props;
    }

    /**
     * Creates and configures a KafkaConsumer instance for OrderCommandEvent.
     * The consumer uses a custom JsonDeserializer configured with a shared ObjectMapper.
     * Type headers are disabled to simplify deserialization when only one message type is expected.
     * Trusted packages are explicitly added to prevent deserialization vulnerabilities.
     *
     * @param consumerProps Kafka consumer properties.
     * @param topicName     Kafka topic to subscribe to.
     * @return A configured KafkaConsumer instance.
     */
    @Override
    protected Consumer<String, OrderCommandEvent> createKafkaConsumer(Properties consumerProps, String topicName) {
        JsonDeserializer<OrderCommandEvent> jsonDeserializer =
                new JsonDeserializer<>(OrderCommandEvent.class, objectMapper);

        // Disable type headers to avoid requiring __TypeId__ in Kafka headers
        jsonDeserializer.setUseTypeHeaders(false);

        // Add trusted packages for secure deserialization
        jsonDeserializer.addTrustedPackages(
                "treiding.hpq.basedomain.kafkaevent",
                "treiding.hpq.basedomain.order"
        );

        return new KafkaConsumer<>(
                consumerProps,
                new StringDeserializer(),
                jsonDeserializer
        );
    }
}


