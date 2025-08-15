package treiding.hpq.basedomain.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * BaseKafkaConsumer is an abstract base class for building Kafka consumers with manual or batch commit support.
 * Key features:
 * - Supports both per-message commit and batch commit modes.
 * - Manages consumer lifecycle with start/stop methods.
 * - Subclasses define KafkaConsumer creation via createKafkaConsumer().
 * - Handles safe shutdown and ensures remaining offsets are committed.
 *
 * @param <T> The type of the deserialized Kafka message value.
 */
public abstract class BaseKafkaConsumer<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final Consumer<String, T> kafkaConsumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    // Commit configuration
    private final boolean commitPerMessage;
    private final int commitBatchSize;

    /**
     * Creates a BaseKafkaConsumer with default commit-per-message behavior.
     * Equivalent to calling BaseKafkaConsumer(consumerProps, topicName, true, 1).
     *
     * @param consumerProps Kafka consumer properties.
     * @param topicName     Kafka topic this consumer will subscribe to.
     */
    public BaseKafkaConsumer(Properties consumerProps, String topicName) {
        this(consumerProps, topicName, true, 1);
    }

    /**
     * Creates a BaseKafkaConsumer with configurable commit behavior.
     *
     * @param consumerProps    Kafka consumer properties.
     * @param topicName        Kafka topic this consumer will subscribe to.
     * @param commitPerMessage If true, commits after each successfully processed message.
     * @param commitBatchSize  Number of messages to process before committing (only applies if commitPerMessage is false).
     */
    public BaseKafkaConsumer(Properties consumerProps, String topicName, boolean commitPerMessage, int commitBatchSize) {
        this.kafkaConsumer = createKafkaConsumer(consumerProps, topicName);
        this.commitPerMessage = commitPerMessage;
        this.commitBatchSize = commitBatchSize;
        log.info("BaseKafkaConsumer initialized for topic: {} (commitPerMessage={}, commitBatchSize={})",
                topicName, commitPerMessage, commitBatchSize);
    }

    /**
     * Creates the specific KafkaConsumer instance used by this consumer.
     * Implementations must specify key/value deserializers and any custom deserialization logic.
     *
     * @param consumerProps Kafka consumer properties.
     * @param topicName     Kafka topic to subscribe to.
     * @return A fully configured KafkaConsumer instance.
     */
    protected abstract Consumer<String, T> createKafkaConsumer(Properties consumerProps, String topicName);

    /**
     * Starts consuming messages from Kafka on a separate thread.
     * Behavior:
     * - Subscribes to the given topic.
     * - Calls the provided eventHandler for each message.
     * - Commits offsets per message or in batches depending on configuration.
     * - Stops when stopConsuming() is called.
     *
     * @param topic        Kafka topic to subscribe to.
     * @param eventHandler Function to process each consumed message.
     */
    public void startConsuming(String topic, java.util.function.Consumer<T> eventHandler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                log.info("Starting to listen for messages on topic: {}", topic);
                kafkaConsumer.subscribe(Collections.singletonList(topic));

                int processedCount = 0;

                try {
                    while (running.get()) { // == while (true)
                        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, T> record : records) {
                            try {
                                // Process the message
                                eventHandler.accept(record.value());

                                if (commitPerMessage) {
                                    // Commit immediately after processing
                                    kafkaConsumer.commitSync(Collections.singletonMap(
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset() + 1)
                                    ));
                                    log.debug("Committed offset {} for partition {}", record.offset() + 1, record.partition());
                                } else {
                                    processedCount++;
                                    if (processedCount >= commitBatchSize) {
                                        kafkaConsumer.commitSync();
                                        log.debug("Committed offsets after {} messages", processedCount);
                                        processedCount = 0;
                                    }
                                }
                            } catch (Exception e) {
                                // Do not commit if processing fails
                                log.error("Error processing record {} - {}", record, e.getMessage(), e);
                            }
                        }
                    }
                } catch (WakeupException e) {
                    log.info("Consumer received shutdown signal.");
                } catch (Exception e) {
                    log.error("Error during Kafka message processing: {}", e.getMessage(), e);
                } finally {
                    try {
                        // Commit remaining offsets before closing
                        kafkaConsumer.commitSync();
                    } catch (Exception commitEx) {
                        log.warn("Failed to commit offsets during shutdown: {}", commitEx.getMessage());
                    }
                    kafkaConsumer.close();
                    log.info("Consumer on topic {} stopped and closed.", topic);
                }
            }, getClass().getSimpleName() + "-ConsumerThread");
            consumerThread.start();
        }
    }

    /**
     * Stops consuming messages gracefully.
     * Behavior:
     * - Signals the consumer thread to stop.
     * - Calls KafkaConsumer.wakeup() to interrupt any ongoing poll().
     * - Waits for the consumer thread to finish before returning.
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            kafkaConsumer.wakeup();
            try {
                if (consumerThread != null) {
                    consumerThread.join();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Consumer thread interrupted while waiting to stop.");
            }
        }
    }
}
