package treiding.hpq.basedomain.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;

public abstract class BaseKafkaProducer<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    // KafkaTemplate sẽ được inject thông qua constructor của các lớp con
    protected final KafkaTemplate<String, T> kafkaTemplate;

    // Constructor để inject KafkaTemplate
    public BaseKafkaProducer(KafkaTemplate<String, T> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends a message to a specified Kafka topic with a given key and payload.
     * Includes logging for successful and failed sends.
     *
     * @param topic The Kafka topic to send the message to.
     * @param key   The key for the Kafka message (e.g., order ID, trade ID).
     * @param payload The message payload (e.g., Order object, Trade object).
     */
    protected void sendMessage(String topic, String key, T payload) {
        log.info("Sending message to topic '{}' with key '{}': {}", topic, key, payload.toString());

        // Using KafkaTemplate's send method, which now returns CompletableFuture
        CompletableFuture<SendResult<String, T>> future = kafkaTemplate.send(topic, key, payload);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.debug("Message sent successfully to topic {} with key {} at offset {}",
                        result.getProducerRecord().topic(),
                        result.getProducerRecord().key(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message to topic {} with key {}: {}", topic, key, ex.getMessage(), ex);
            }
        });
    }
}