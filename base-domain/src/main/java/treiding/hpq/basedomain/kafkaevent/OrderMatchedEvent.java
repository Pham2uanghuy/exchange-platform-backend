package treiding.hpq.basedomain.kafkaevent;

import java.math.BigDecimal;
import java.time.Instant;

public class OrderMatchedEvent {
    private String tradeId;
    private String aggressorOrderId;
    private String passiveOrderId;
    private BigDecimal matchedQuantity;
    private BigDecimal price;
    private Instant timestamp; // Timestamp of when the match occurred

    // Constructors
    public OrderMatchedEvent() {}

    public OrderMatchedEvent(String tradeId, String aggressorOrderId, String passiveOrderId,
                             BigDecimal matchedQuantity, BigDecimal price, Instant timestamp) {
        this.tradeId = tradeId;
        this.aggressorOrderId = aggressorOrderId;
        this.passiveOrderId = passiveOrderId;
        this.matchedQuantity = matchedQuantity;
        this.price = price;
        this.timestamp = timestamp;
    }

    // Getters
    public String getTradeId() {
        return tradeId;
    }

    public String getAggressorOrderId() {
        return aggressorOrderId;
    }

    public String getPassiveOrderId() {
        return passiveOrderId;
    }

    public BigDecimal getMatchedQuantity() {
        return matchedQuantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public void setAggressorOrderId(String aggressorOrderId) {
        this.aggressorOrderId = aggressorOrderId;
    }

    public void setPassiveOrderId(String passiveOrderId) {
        this.passiveOrderId = passiveOrderId;
    }

    public void setMatchedQuantity(BigDecimal matchedQuantity) {
        this.matchedQuantity = matchedQuantity;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderMatchedEvent{" +
                "tradeId='" + tradeId + '\'' +
                ", aggressorOrderId='" + aggressorOrderId + '\'' +
                ", passiveOrderId='" + passiveOrderId + '\'' +
                ", matchedQuantity=" + matchedQuantity +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }
}
