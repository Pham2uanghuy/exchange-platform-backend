package treiding.hpq.basedomain.kafkaevent;

import java.time.Instant;

public class OrderCancellationRejectedEvent {
    private String orderId;
    private CancellationReason reason;
    private Instant timestamp;

    public OrderCancellationRejectedEvent() {
    }

    public OrderCancellationRejectedEvent(String orderId, CancellationReason reason) {
        this.orderId = orderId;
        this.timestamp = Instant.now();
        this.reason = reason;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public CancellationReason getReason() {
        return reason;
    }

    public void setReason(CancellationReason reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "OrderCancellationRejectedEvent{" +
                "orderId='" + orderId + '\'' +
                ", reason='" + reason + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}