package treiding.hpq.matchingservice.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer; // Import the Spring JsonDeserializer
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class OrderInitialLoadConsumer extends BaseKafkaConsumer<Order> {

    private static final long POLL_TIMEOUT_MS = 500;
    private static final int EMPTY_POLLS_THRESHOLD = 3;
    private static final String ORDER_INITIAL_LOAD_TOPIC = "order.initial-load";


    // Inject Kafka properties from application.properties
    public OrderInitialLoadConsumer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                    @Value("${spring.kafka.consumer.group-id.initial-load}") String groupId) {
        // Create the Properties object here and pass it to the super constructor.
        super(buildConsumerProps(bootstrapServers, groupId), ORDER_INITIAL_LOAD_TOPIC);
    }

    /**
     * Helper method to build Kafka consumer properties specifically for the initial load process.
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID for this specific initial load.
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

        props.put("auto.offset.reset", "earliest"); // Start reading from the beginning of the topic
        props.put("enable.auto.commit", "false"); // Do not auto-commit offsets for initial load
        return props;
    }

    @Override
    protected Consumer<String, Order> createKafkaConsumer(Properties consumerProps, String topicName) {
        // This method simply uses the 'consumerProps' already built to create the KafkaConsumer.
        // The topicName parameter here is technically redundant for this specific implementation
        // because we are always assigning partitions based on the initial load strategy.
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Consumes all orders from the specified Kafka topic until the "end" of the topic is reached.
     * This method is designed for a one-time, full topic read at startup.
     * @param topicName The name of the Kafka topic to consume from (e.g., "order.initial-load").
     * @return A list of all Order objects loaded from the topic.
     */
    public List<Order> consumeAllOrdersUntilEndOfTopic(String topicName) {
        List<Order> loadedOrders = new ArrayList<>();
        log.info("Starting to load all orders from topic '{}' for initial load.", topicName);

        // 1. Assign consumer to the topic and all its partitions
        List<TopicPartition> partitions = kafkaConsumer.partitionsFor(topicName).stream()
                .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                .collect(Collectors.toList());

        if (partitions.isEmpty()) {
            log.warn("No partitions found for topic '{}'. This might indicate a misconfiguration or topic not yet created.", topicName);
            return loadedOrders;
        }

        kafkaConsumer.assign(partitions);

        // 2. Seek the consumer to the beginning of all assigned partitions
        kafkaConsumer.seekToBeginning(partitions);

        int emptyPolls = 0;
        long totalRecords = 0;

        try {
            while (true) {
                ConsumerRecords<String, Order> records = kafkaConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    emptyPolls++;
                    log.debug("Empty poll #{}. Total records loaded: {}", emptyPolls, totalRecords);
                    if (emptyPolls >= EMPTY_POLLS_THRESHOLD) {
                        log.info("Reached end of topic '{}' after {} consecutive empty polls. Total orders loaded: {}",
                                topicName, EMPTY_POLLS_THRESHOLD, totalRecords);
                        break;
                    }
                } else {
                    emptyPolls = 0;
                    totalRecords += records.count();
                    log.debug("Received {} new messages. Total orders loaded: {}", records.count(), totalRecords);
                    for (var record : records) {
                        loadedOrders.add(record.value());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error loading orders from Kafka for initial load: {}", e.getMessage(), e);
        } finally {
            kafkaConsumer.close();
            log.info("Initial load consumer for topic '{}' closed.", topicName);
        }
        return loadedOrders;
    }
}