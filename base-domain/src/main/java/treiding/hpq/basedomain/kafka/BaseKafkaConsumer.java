package treiding.hpq.basedomain.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class BaseKafkaConsumer<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final Consumer<String, T> kafkaConsumer; // Assumes String key

    // Used for continuously running consumers
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    /**
     * Base constructor for Kafka consumers.
     * Subclasses will provide the specific Kafka properties and the topic name.
     *
     * @param consumerProps The Kafka consumer properties (e.g., bootstrap servers, deserializers, group ID).
     * @param topicName     The Kafka topic that this consumer instance will subscribe to.
     */
    public BaseKafkaConsumer(Properties consumerProps, String topicName) {
        // Here, we call the abstract method to create the specific KafkaConsumer instance,
        // passing along the provided properties and topic name.
        this.kafkaConsumer = createKafkaConsumer(consumerProps, topicName);
        log.info("BaseKafkaConsumer initialized for topic: {}", topicName);
    }

    /**
     * Abstract method to create the specific KafkaConsumer instance (e.g., with a custom deserializer).
     * This method must be implemented by concrete consumer classes to define how their KafkaConsumer is built.
     *
     * @param consumerProps The properties for the Kafka consumer.
     * @param topicName     The topic to consume from.
     * @return An initialized KafkaConsumer.
     */
    protected abstract Consumer<String, T> createKafkaConsumer(Properties consumerProps, String topicName);

    /**
     * Starts the consumer to process messages continuously.
     * Typically used for consumers listening to real-time events.
     *
     * @param topic       The Kafka topic to subscribe to.
     * @param eventHandler A functional interface to process each received event.
     */
    public void startConsuming(String topic, java.util.function.Consumer<T> eventHandler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                log.info("Starting to listen for messages on topic: {}", topic);
                kafkaConsumer.subscribe(Collections.singletonList(topic));
                try {
                    while (running.get()) {
                        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.ofMillis(100)); // Poll for 100ms
                        records.forEach(record -> {
                            handleMessage(record.value(), eventHandler);
                        });
                    }
                } catch (WakeupException e) {
                    // Called when consumer.wakeup() is invoked from another thread
                    log.info("Consumer received shutdown signal.");
                } catch (Exception e) {
                    log.error("Error during Kafka message processing: {}", e.getMessage(), e);
                } finally {
                    kafkaConsumer.close();
                    log.info("Consumer on topic {} stopped and closed.", topic);
                }
            }, getClass().getSimpleName() + "-ConsumerThread");
            consumerThread.start();
        }
    }

    /**
     * Handles a message received from Kafka.
     * This method is called internally by the consumer thread.
     *
     * @param event       The event object deserialized from Kafka.
     * @param eventHandler A functional interface to process the event.
     */
    protected void handleMessage(T event, java.util.function.Consumer<T> eventHandler) {
        log.debug("Received event of type {} from Kafka: {}", event.getClass().getSimpleName(), event);
        try {
            // Call the provided eventHandler to process the message.
            eventHandler.accept(event);
        } catch (Exception e) {
            log.error("Error processing event: {} - {}", event, e.getMessage(), e);
        }
    }

    /**
     * Stops a continuously running consumer gracefully.
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            kafkaConsumer.wakeup(); // Interrupts the poll() method
            try {
                if (consumerThread != null) {
                    consumerThread.join(); // Wait for the thread to finish
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Consumer thread interrupted while waiting to stop.");
            }
        }
    }
}