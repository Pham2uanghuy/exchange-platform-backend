package treiding.hpq.matchingservice.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCommandEvent;
import treiding.hpq.matchingservice.entity.OrderBook;
import treiding.hpq.matchingservice.kafka.*;

import java.util.*;


@Service
public class MatchingService {

    private static final Logger log = LoggerFactory.getLogger(MatchingService.class);

    private final OrderBookManager orderBookManager;
    private final Map<String, MatchingEngine> matchingEngines;

    private final OrderCommandConsumer orderCommandConsumer;
    private final OrderMatchedEventProducer orderMatchedEventProducer;
    private final TradeEventProducer tradeProducer;
    private final OrderCancellationProducer orderCancellationProducer;

    /**
     * Constructs the MatchingService with all required dependencies.
     *
     * @param orderBookManager        Service to manage and retrieve order books.
     * @param orderCommandConsumer    Kafka consumer for handling order command events.
     * @param orderMatchedEventProducer Kafka producer to send matched order events.
     * @param tradeProducer           Kafka producer to send trade events.
     * @param orderCancellationProducer Kafka producer to send order cancellation events.
     */
    public MatchingService(
            @Lazy OrderBookManager orderBookManager,
            OrderCommandConsumer orderCommandConsumer,
            OrderMatchedEventProducer orderMatchedEventProducer,
            TradeEventProducer tradeProducer,
            OrderCancellationProducer orderCancellationProducer) {

        this.orderBookManager = orderBookManager;
        this.orderCommandConsumer = orderCommandConsumer;
        this.orderMatchedEventProducer = orderMatchedEventProducer;
        this.tradeProducer = tradeProducer;
        this.orderCancellationProducer = orderCancellationProducer;
        this.matchingEngines = new HashMap<>();
    }

    /**
     * Initializes the matching engines and starts Kafka listeners.
     * This method is called automatically after Spring finishes dependency injection.
     */
    @PostConstruct
    public void init() {
        log.info("[MatchingService] Initializing engines...");
        initializeMatchingEngines();

        log.info("[MatchingService] Starting Kafka listener for order commands");
        startKafkaListeners(); // Direct call, no extra thread needed
    }

    /**
     * Gracefully shuts down the service when the application is stopping.
     * Stops Kafka consumers and matching engines to release resources.
     */
    @PreDestroy
    public void shutdown() {
        log.info("[MatchingService] Initiating graceful shutdown of Kafka listeners.");
        orderCommandConsumer.stopConsuming();

        log.info("[MatchingService] Shutting down MatchingEngines...");
        matchingEngines.values().forEach(MatchingEngine::shutdown);
        log.info("[MatchingService] Shutdown complete.");
    }

    /**
     * Initializes MatchingEngine instances for each available instrument in the order book.
     * Each instrument gets its own independent MatchingEngine instance.
     */
    private void initializeMatchingEngines() {
        for (String instrumentId : orderBookManager.getInstrumentIds()) {
            OrderBook orderBook = orderBookManager.getOrderBook(instrumentId);
            if (orderBook != null) {
                MatchingEngine engine = new MatchingEngine(
                        orderBook,
                        orderMatchedEventProducer,
                        tradeProducer,
                        orderCancellationProducer
                );
                matchingEngines.put(instrumentId, engine);
                log.info("[MatchingService] Initialized MatchingEngine for instrument: {}", instrumentId);
            } else {
                log.error("[MatchingService] Could not find OrderBook for instrument: {}. Skipping engine initialization.", instrumentId);
            }
        }
    }

    /**
     * Starts Kafka listeners for order commands.
     * Listens to the "order.events" topic and delegates message handling
     * to the {@link #handleOrderCommand(OrderCommandEvent)} method.
     */
    private void startKafkaListeners() {
        log.info("[MatchingService] Starting real-time Kafka listeners for order events.");
        orderCommandConsumer.startConsuming("order.events", this::handleOrderCommand);
    }

    /**
     * Handles incoming order command events from Kafka.
     * <p>
     * Depending on the event type, this method will either enqueue the order
     * for matching, cancel it from the order book, or ignore it if not relevant.
     * </p>
     *
     * @param eventMessage The order command event received from Kafka.
     */
    private void handleOrderCommand(OrderCommandEvent eventMessage) {
        Order eventOrder = eventMessage.getOrder();
        String eventType = eventMessage.getEventType();
        String instrumentId = eventOrder.getInstrumentId();

        MatchingEngine engine = matchingEngines.get(instrumentId);

        log.info("[MatchingService] Received Kafka event: EventType={}, OrderId={}",
                eventType, eventOrder.getOrderId());

        if ("OrderCreated".equals(eventType) ||
                ("OrderUpdated".equals(eventType) && eventOrder.getStatus() == OrderStatus.OPEN)) {
            engine.addToOrderQueue(eventOrder);
            log.info("[MatchingService] Enqueued order {} for matching.", eventOrder.getOrderId());

        } else if ("OrderCancelled".equals(eventType)) {
            log.info("[MatchingService] Received cancellation for order {}.", eventOrder.getOrderId());
            Order orderToCancel = engine.getOpenOrderById(eventOrder.getOrderId());
            if (orderToCancel != null) {
                engine.cancelOrder(eventOrder.getOrderId());
                log.info("[MatchingService] Order {} cancelled in OrderBook.", eventOrder.getOrderId());
            } else {
                log.info("[MatchingService] Order {} not found as OPEN. No cancellation performed.", eventOrder.getOrderId());
            }
        } else {
            log.debug("[MatchingService] EventType={} for order {} ignored (status={}).",
                    eventType, eventOrder.getOrderId(), eventOrder.getStatus());
        }
    }
}
