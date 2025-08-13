package treiding.hpq.marketdataservice.model;

import treiding.hpq.basedomain.entity.Trade;

import java.math.BigDecimal;
import java.time.Instant;

public class MarketDataUpdate {

    private UpdateType type;
    private String instrumentId;
    private Object data;
    private Instant timestamp;

    // Helper constructors for specific update types
    public static MarketDataUpdate tradeUpdate(Trade trade) {
        return new MarketDataUpdate(UpdateType.TRADE_UPDATE, trade.getInstrumentId(), trade, Instant.now());
    }

    public static MarketDataUpdate lastPriceUpdate(String instrumentId, BigDecimal lastPrice) {
        return new MarketDataUpdate(UpdateType.LAST_PRICE_UPDATE, instrumentId, lastPrice, Instant.now());
    }


    public MarketDataUpdate() {}


    public MarketDataUpdate(UpdateType type, String instrumentId, Object data, Instant timestamp) {
        this.type = type;
        this.instrumentId = instrumentId;
        this.data = data;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public UpdateType getType() {
        return type;
    }

    public void setType(UpdateType type) {
        this.type = type;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
