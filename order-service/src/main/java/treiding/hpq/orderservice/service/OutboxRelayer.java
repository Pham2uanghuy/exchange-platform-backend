package treiding.hpq.orderservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.orderservice.outbox.OutboxEvent;
import treiding.hpq.orderservice.outbox.OutboxStatus;
import treiding.hpq.orderservice.repository.OutboxEventRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;



/**
 * Service responsible for relaying outbox events to Kafka.
 * This class implements the Outbox Pattern, which helps ensure consistency between
 * database state changes and external message publishing (e.g., to Kafka).
 * It periodically scans the `outbox_events` table for PENDING events and attempts to send them
 * to their designated Kafka topics. Events are marked as SENT or FAILED accordingly.
 */
@Service
public class OutboxRelayer {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayer.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Constructor with required dependencies.
     *
     * @param outboxEventRepository repository for querying and updating outbox events.
     * @param kafkaTemplate template used to send messages to Kafka.
     * @param objectMapper JSON deserializer for converting payloads.
     */
    public OutboxRelayer(OutboxEventRepository outboxEventRepository,
                         KafkaTemplate<String, Object> kafkaTemplate,
                         ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Scheduled task that scans for PENDING outbox events and sends them to Kafka.
     *
     * This method:
     * 1. Fetches a batch of PENDING events.
     * 2. Validates that the event is still unprocessed.
     * 3. Deserializes the payload and sends it to Kafka.
     * 4. Updates the event status to SENT or FAILED based on send result.
     *
     * The delay between executions is configured via application properties.
     * This method runs inside a single transaction to ensure atomicity of read + status update.
     */
    @Scheduled(fixedDelayString = "${app.outbox.relayer.fixed-delay-ms}")
    @Transactional
    public void processOutboxEvents() {
        log.debug("OutboxRelayer: Checking for pending events...");

        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (pendingEvents.isEmpty()) {
            log.debug("OutboxRelayer: No pending events found.");
            return;
        }

        log.info("OutboxRelayer: Found {} pending events to process.", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                // Ensure the event is still PENDING (not already processed)
                Optional<OutboxEvent> currentEventOptional = outboxEventRepository.findById(event.getId());
                if (currentEventOptional.isEmpty() || currentEventOptional.get().getStatus() != OutboxStatus.PENDING) {
                    log.debug("OutboxRelayer: Event {} is no longer PENDING or does not exist. Skipping.", event.getId());
                    continue;
                }

                OutboxEvent eventToProcess = currentEventOptional.get();
                Object payloadObject = deserializePayload(eventToProcess);

                log.debug("OutboxRelayer: Sending event ID {} (Type: {}) to topic '{}' with key '{}'",
                        eventToProcess.getId(), eventToProcess.getEventType(), eventToProcess.getTopic(), eventToProcess.getAggregateId());

                // Send the event asynchronously to Kafka
                CompletableFuture<SendResult<String, Object>> future =
                        kafkaTemplate.send(eventToProcess.getTopic(), eventToProcess.getAggregateId(), payloadObject);

                // Handle the Kafka send result asynchronously
                future.whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("OutboxRelayer: Event ID {} sent successfully to Kafka.", eventToProcess.getId());
                        markEventAsSent(eventToProcess.getId());
                    } else {
                        log.error("OutboxRelayer: Failed to send event ID {} to Kafka: {}", eventToProcess.getId(), ex.getMessage(), ex);
                        markEventAsFailed(eventToProcess.getId(), ex.getMessage());
                    }
                });

            } catch (JsonProcessingException e) {
                log.error("OutboxRelayer: JSON deserialization error for event ID {}: {}", event.getId(), e.getMessage(), e);
                markEventAsFailed(event.getId(), "JSON Deserialization Error: " + e.getMessage());
            } catch (Exception e) {
                log.error("OutboxRelayer: Unexpected error processing event ID {}: {}", event.getId(), e.getMessage(), e);
                markEventAsFailed(event.getId(), "Unexpected Error: " + e.getMessage());
            }
        }
    }

    /**
     * Deserializes the event payload into a domain object.
     *
     * This method maps event types to their corresponding Java class for correct deserialization.
     *
     * @param event The outbox event containing the JSON payload.
     * @return A deserialized domain object (e.g., Order).
     * @throws JsonProcessingException if the JSON cannot be parsed.
     */
    private Object deserializePayload(OutboxEvent event) throws JsonProcessingException {
        switch (event.getEventType()) {
            case "OrderCreated":
            case "OrderCancelled":
                return objectMapper.readValue(event.getPayload(), Order.class);
            default:
                log.warn("Unknown event type '{}' for event ID {}. Attempting to deserialize as generic Object.",
                        event.getEventType(), event.getId());
                return objectMapper.readValue(event.getPayload(), Object.class);
        }
    }

    /**
     * Marks an event as successfully sent to Kafka.
     *
     * This method runs in a new transaction to avoid issues with callback thread context.
     *
     * @param eventId ID of the event to mark as SENT.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markEventAsSent(String eventId) {
        log.debug("Attempting to mark OutboxEvent {} as SENT.", eventId);
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            if (event.getStatus() == OutboxStatus.PENDING) {
                event.setStatus(OutboxStatus.SENT);
                event.setProcessedAt(Instant.now());
                event.setErrorMessage(null);
                outboxEventRepository.save(event);
                log.info("OutboxEvent {} successfully marked as SENT.", eventId);
            } else {
                log.debug("OutboxEvent {} was already processed (status: {}), skipping SENT update.", eventId, event.getStatus());
            }
        });
    }

    /**
     * Marks an event as FAILED if it could not be sent.
     *
     * Also runs in a new transaction for isolation from the calling context.
     *
     * @param eventId      ID of the event to mark as FAILED.
     * @param errorMessage Error message to store in the database.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markEventAsFailed(String eventId, String errorMessage) {
        log.debug("Attempting to mark OutboxEvent {} as FAILED.", eventId);
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            if (event.getStatus() == OutboxStatus.PENDING) {
                event.setStatus(OutboxStatus.FAILED);
                event.setProcessedAt(Instant.now());
                event.setErrorMessage(errorMessage != null
                        ? errorMessage.substring(0, Math.min(errorMessage.length(), 255))
                        : "Unknown error");
                outboxEventRepository.save(event);
                log.warn("OutboxEvent {} marked as FAILED with error: {}", eventId, errorMessage);
            } else {
                log.debug("OutboxEvent {} was already processed (status: {}), skipping FAILED update.", eventId, event.getStatus());
            }
        });
    }
}
