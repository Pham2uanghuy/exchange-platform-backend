package treiding.hpq.orderservice.outbox;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.persistence.*;
import treiding.hpq.basedomain.entity.Order;

import java.time.Instant;
import java.util.UUID;

/**
 * Entity representing an event stored in the Outbox table.
 * This pattern helps achieve reliable event publishing in distributed systems (Outbox Pattern).
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    /**
     * Unique identifier for the event (UUID).
     */
    @Id
    private String id;

    /**
     * Timestamp when the event was created.
     */
    @Column(nullable = false)
    private Instant createdAt;

    /**
     * Type of the aggregate this event belongs to (e.g., "Order").
     */
    @Column(nullable = false)
    private String aggregateType;

    /**
     * Unique identifier of the aggregate (e.g., orderId).
     */
    @Column(nullable = false)
    private String aggregateId;

    /**
     * Type of event (e.g., "OrderCreated", "OrderCancelled").
     */
    @Column(nullable = false)
    private String eventType;

    /**
     * JSON-serialized event payload (usually a snapshot of the aggregate).
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    /**
     * Kafka topic to which the event should be published.
     */
    @Column(nullable = false)
    private String topic;

    /**
     * Status of the event (e.g., PENDING, SENT, FAILED).
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    /**
     * Timestamp when the event was processed (sent to Kafka).
     */
    @Column(nullable = true)
    private Instant processedAt;

    /**
     * Error message in case the event processing fails.
     */
    @Column(nullable = true)
    private String errorMessage;

    /**
     * Shared ObjectMapper for serializing event payloads.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    /**
     * Default constructor.
     * Initializes the event with a UUID and sets default status to PENDING.
     */
    public OutboxEvent() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.status = OutboxStatus.PENDING;
    }

    /**
     * Static factory method to create an OutboxEvent for "OrderCreated".
     *
     * @param order The order entity to serialize.
     * @param topic Kafka topic to publish to.
     * @return A new OutboxEvent instance.
     */
    public static OutboxEvent createOrderCreatedEvent(Order order, String topic) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateType("Order");
        event.setAggregateId(order.getOrderId());
        event.setEventType("OrderCreated");
        event.setTopic(topic);
        try {
            event.setPayload(objectMapper.writeValueAsString(order));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting Order to JSON for OutboxEvent", e);
        }
        return event;
    }

    /**
     * Static factory method to create an OutboxEvent for "OrderCancelled".
     *
     * @param order The order entity to serialize.
     * @param topic Kafka topic to publish to.
     * @return A new OutboxEvent instance.
     */
    public static OutboxEvent createOrderCancelledEvent(Order order, String topic) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateType("Order");
        event.setAggregateId(order.getOrderId());
        event.setEventType("OrderCancelled");
        event.setTopic(topic);
        try {
            event.setPayload(objectMapper.writeValueAsString(order));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting Order to JSON for OutboxEvent", e);
        }
        return event;
    }

    // --- Getters and Setters ---

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public Instant getCreatedAt() { return createdAt; }

    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public String getAggregateType() { return aggregateType; }

    public void setAggregateType(String aggregateType) { this.aggregateType = aggregateType; }

    public String getAggregateId() { return aggregateId; }

    public void setAggregateId(String aggregateId) { this.aggregateId = aggregateId; }

    public String getEventType() { return eventType; }

    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getPayload() { return payload; }

    public void setPayload(String payload) { this.payload = payload; }

    public String getTopic() { return topic; }

    public void setTopic(String topic) { this.topic = topic; }

    public OutboxStatus getStatus() { return status; }

    public void setStatus(OutboxStatus status) { this.status = status; }

    public Instant getProcessedAt() { return processedAt; }

    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }

    public String getErrorMessage() { return errorMessage; }

    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    @Override
    public String toString() {
        return "OutboxEvent{" +
                "id='" + id + '\'' +
                ", aggregateType='" + aggregateType + '\'' +
                ", aggregateId='" + aggregateId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", topic='" + topic + '\'' +
                ", status=" + status +
                '}';
    }
}
