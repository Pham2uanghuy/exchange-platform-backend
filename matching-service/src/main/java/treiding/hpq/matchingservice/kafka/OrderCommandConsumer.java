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

    // Shared ObjectMapper instance for better performance and correct handling of Java 8 time types
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Constructs the OrderCommandConsumer with Kafka configuration injected by Spring.
     *
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID.
     */
    public OrderCommandConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                @Value("${spring.kafka.consumer.group-id.order-command}") String groupId) {
        super(buildConsumerProps(bootstrapServers, groupId), ORDER_EVENTS_TOPIC);
    }

    /**
     * Builds Kafka consumer configuration properties.
     *
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID.
     * @return Kafka consumer properties.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());

        // Trusted packages for deserialization – keep specific and avoid wildcards in production
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "treiding.hpq.basedomain.kafkaevent,treiding.hpq.basedomain.order");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCommandEvent.class.getName());

        // Start consuming from latest offset (recommended for real-time processing)
        props.put("auto.offset.reset", "latest");

        // Enable auto offset commit (can be set to false for manual commit if stronger guarantees are needed)
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "5000");

        return props;
    }

    /**
     * Creates and configures a KafkaConsumer instance.
     *
     * @param consumerProps Kafka properties.
     * @param topicName Kafka topic to subscribe to.
     * @return Configured KafkaConsumer instance.
     */
    @Override
    protected Consumer<String, OrderCommandEvent> createKafkaConsumer(Properties consumerProps, String topicName) {
        JsonDeserializer<OrderCommandEvent> jsonDeserializer =
                new JsonDeserializer<>(OrderCommandEvent.class, objectMapper);

        // Disable type headers if the topic contains only a single event type
        jsonDeserializer.setUseTypeHeaders(false);

        // Explicitly trust the same packages as configured in properties
        jsonDeserializer.addTrustedPackages("treiding.hpq.basedomain.kafkaevent", "treiding.hpq.basedomain.order");

        return new KafkaConsumer<>(
                consumerProps,
                new StringDeserializer(),
                jsonDeserializer
        );
    }
}