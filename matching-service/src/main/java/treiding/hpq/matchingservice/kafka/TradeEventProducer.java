package treiding.hpq.matchingservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;


@Component
public class TradeEventProducer extends BaseKafkaProducer<Trade> {

    private static final String TRADE_EVENTS_TOPIC = "trade.events";

    public TradeEventProducer(KafkaTemplate<String, Trade> kafkaTemplate) {
        super(kafkaTemplate);
    }

    /**
     * Sends a trade event to Kafka.
     * The key for the message will be the trade's ID.
     *
     * @param trade The Trade object representing the executed trade.
     */
    public void sendTradeEvent(Trade trade) {
        if (trade.getTradeId() == null) {
            log.error("Cannot send trade event: Trade ID is null for trade {}", trade);
            return;
        }
        sendMessage(TRADE_EVENTS_TOPIC, trade.getTradeId(), trade);
    }
}