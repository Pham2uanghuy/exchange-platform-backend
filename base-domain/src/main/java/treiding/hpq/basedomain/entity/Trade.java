package treiding.hpq.basedomain.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents a completed trade between a buy and a sell order.
 * Stores trade details such as price, quantity, and involved order IDs.
 */
@Entity
@Table(name = "trades")
public class Trade {

    /**
     * Internal database ID for persistence.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    /**
     * Unique identifier for the trade (used externally in the system).
     */
    @Column(unique = true, nullable = false)
    private String tradeId;

    /**
     * Order ID of the buyer.
     */
    private String buyerOrderId;

    /**
     * Order ID of the seller.
     */
    private String sellerOrderId;

    /**
     * User Id of the buyer
     */
    private String buyerId;

    /**
     * User Id of the seller
     */
    private String sellerId;

    /**
     * Identifier of the instrument being traded (e.g., BTC-USD).
     */
    private String instrumentId;

    /**
     * Price at which the trade occurred.
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal tradedPrice;

    /**
     * Quantity that was traded.
     */
    @Column(precision = 19, scale = 8)
    private BigDecimal tradedQuantity;

    /**
     * Timestamp of when the trade was executed.
     */
    private Instant timestamp;

    // Constructors, getters, setters (omitted for brevity)


    public Trade() {
    }

    public Trade(String tradeId, String buyerOrderId, String buyerId, String sellerOrderId, String sellerId, String instrumentId, BigDecimal tradedPrice, BigDecimal tradedQuantity, Instant timestamp) {
        this.tradeId = tradeId;
        this.buyerOrderId = buyerOrderId;
        this.sellerOrderId = sellerOrderId;
        this.instrumentId = instrumentId;
        this.tradedPrice = tradedPrice;
        this.tradedQuantity = tradedQuantity;
        this.timestamp = timestamp;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
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

    public BigDecimal getTradedQuantity() {
        return tradedQuantity;
    }

    public void setTradedQuantity(BigDecimal tradedQuantity) {
        this.tradedQuantity = tradedQuantity;
    }

    public BigDecimal getTradedPrice() {
        return tradedPrice;
    }

    public void setTradedPrice(BigDecimal tradedPrice) {
        this.tradedPrice = tradedPrice;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }

    public String getSellerOrderId() {
        return sellerOrderId;
    }

    public void setSellerOrderId(String sellerOrderId) {
        this.sellerOrderId = sellerOrderId;
    }

    public String getBuyerOrderId() {
        return buyerOrderId;
    }

    public void setBuyerOrderId(String buyerOrderId) {
        this.buyerOrderId = buyerOrderId;
    }

    public String getTradeId() {
        return tradeId;
    }

    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public String getSellerId() {
        return sellerId;
    }

    public void setSellerId(String sellerId) {
        this.sellerId = sellerId;
    }
}