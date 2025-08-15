package treiding.hpq.basedomain.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents an order in the trading system.
 * Each order includes pricing, quantity, status, and trading direction.
 */
@Entity
@Table(name = "orders")
public class Order {

    /**
     * Internal database ID for persistence.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    /**
     * Unique identifier for the order (external-facing).
     */
    @Column(unique = true, nullable = false)
    private String orderId;

    /**
     * ID of the user who placed the order.
     */
    private String userId;

    /**
     * Identifier of the instrument being traded (e.g., BTC-USD).
     */
    private String instrumentId;

    /**
     * Price per unit for the order.
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal price;

    /**
     * Initial quantity specified when the order was placed.
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal originalQuantity;

    /**
     * Quantity that has been filled (executed).
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal filledQuantity = BigDecimal.ZERO;

    /**
     * Quantity remaining to be filled.
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal remainingQuantity;

    /**
     * Direction of the order: BUY or SELL.
     */
    @Enumerated(EnumType.STRING)
    private OrderSide side;

    /**
     * Type of order, such as LIMIT or MARKET.
     * @see OrderType
     */
    @Enumerated(EnumType.STRING)
    private OrderType type;

    /**
     * Current status of the order.
     * @see OrderStatus
     */
    @Enumerated(EnumType.STRING)
    private OrderStatus status = OrderStatus.OPEN;

    /**
     * Timestamp of when the order was created.
     */
    private Instant timestamp;

    /**
     * Version field for optimistic locking.
     * JPA automatically manages this field.
     */
    @Version
    @Column(name = "version")
    private Integer version;

    // this contructor is used in RedisOrderBook
    public Order(Long id, String orderId, String instrumentId, String userId, OrderSide side, BigDecimal price, BigDecimal originalQuantity, BigDecimal remainingQuantity, OrderStatus status, Instant timestamp) {
        this.id = id;
        this.orderId = orderId;
        this.instrumentId = instrumentId;
        this.userId = userId;
        this.side = side;
        this.price = price;
        this.originalQuantity = originalQuantity;
        this.remainingQuantity = remainingQuantity;
        this.status = status;
        this.timestamp = timestamp;
    }

    /**
     * Applies a fill (partial or full) to this order and updates its status.
     *
     * @param fillAmount the quantity that has been filled
     */
    public void fill(BigDecimal fillAmount) {
        if (fillAmount == null || fillAmount.compareTo(BigDecimal.ZERO) <= 0) return;

        this.filledQuantity = this.filledQuantity.add(fillAmount);
        this.remainingQuantity = this.remainingQuantity.subtract(fillAmount);

        if (this.remainingQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            this.status = OrderStatus.FILLED;
            this.remainingQuantity = BigDecimal.ZERO;
        } else {
            this.status = OrderStatus.PARTIALLY_FILLED;
        }
    }

    // Getters, setters, and constructors

    public Order() {
    }

    public Order(Long id, Instant timestamp, OrderStatus status, OrderType type, OrderSide side, BigDecimal remainingQuantity, BigDecimal filledQuantity, BigDecimal originalQuantity, BigDecimal price, String instrumentId, String userId, String orderId) {
        this.id = id;
        this.timestamp = timestamp;
        this.status = status;
        this.type = type;
        this.side = side;
        this.remainingQuantity = remainingQuantity;
        this.filledQuantity = filledQuantity;
        this.originalQuantity = originalQuantity;
        this.price = price;
        this.instrumentId = instrumentId;
        this.userId = userId;
        this.orderId = orderId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public OrderType getType() {
        return type;
    }

    public void setType(OrderType type) {
        this.type = type;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public OrderSide getSide() {
        return side;
    }

    public void setSide(OrderSide side) {
        this.side = side;
    }

    public BigDecimal getRemainingQuantity() {
        return remainingQuantity;
    }

    public void setRemainingQuantity(BigDecimal remainingQuantity) {
        this.remainingQuantity = remainingQuantity;
    }

    public BigDecimal getFilledQuantity() {
        return filledQuantity;
    }

    public void setFilledQuantity(BigDecimal filledQuantity) {
        this.filledQuantity = filledQuantity;
    }

    public BigDecimal getOriginalQuantity() {
        return originalQuantity;
    }

    public void setOriginalQuantity(BigDecimal originalQuantity) {
        this.originalQuantity = originalQuantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }
}
