package treiding.hpq.orderservice.outbox;

/**
 * Enum representing the status of an OutboxEvent.
 * Used to track the lifecycle of an event in the Outbox Pattern.
 */
public enum OutboxStatus {

    /**
     * The event is newly created and has not been processed yet.
     * Awaiting to be sent to the message broker (e.g., Kafka).
     */
    PENDING,

    /**
     * The event has been successfully sent to the message broker.
     */
    SENT,

    /**
     * The event failed to be sent. May require retries or manual intervention.
     */
    FAILED
}