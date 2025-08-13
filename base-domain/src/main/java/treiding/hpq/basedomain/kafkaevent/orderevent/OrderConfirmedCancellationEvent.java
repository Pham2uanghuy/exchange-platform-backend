package treiding.hpq.basedomain.kafkaevent.orderevent;

import java.time.Instant;

public class OrderConfirmedCancellationEvent {
    private String orderId;
    private Instant timestamp;



    public OrderConfirmedCancellationEvent() {
    }

    public OrderConfirmedCancellationEvent(String orderId, String reason) {
        this.orderId = orderId;
        this.timestamp = Instant.now();
    }


    public String getOrderId() {
        return orderId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }


    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }


    @Override
    public String toString() {
        return "OrderConfirmedCancellationEvent{" +
                "orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}