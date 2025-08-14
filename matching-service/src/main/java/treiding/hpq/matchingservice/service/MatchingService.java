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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Service
public class MatchingService {

    private static final Logger log = LoggerFactory.getLogger(MatchingService.class);

    private final OrderBookManager orderBookManager;
    private final Map<String, MatchingEngine> matchingEngines;

    private final OrderCommandConsumer orderCommandConsumer; // Consumer for real-time order events
    private final OrderMatchedEventProducer orderMatchedEventProducer;
    private final TradeEventProducer tradeProducer;
    private final OrderCancellationProducer orderCancellationProducer;
    // A dedicated executor for Kafka listeners to run in the background
    // This allows the main application thread to continue starting up.
    private final ExecutorService kafkaListenerExecutor;


    public MatchingService(@Lazy OrderBookManager orderBookManager,
                           OrderCommandConsumer orderCommandConsumer,
                           OrderMatchedEventProducer orderMatchedEventProducer,
                           TradeEventProducer tradeProducer,
                           OrderCancellationProducer orderCancellationProducer, OrderMatchedEventProducer orderMatchedEventProducer1, TradeEventProducer tradeProducer1, OrderCancellationProducer orderCancellationProducer1) {
        this.orderBookManager = orderBookManager;

        this.orderCommandConsumer = orderCommandConsumer; // Initialize the real-time consumer
        this.orderMatchedEventProducer = orderMatchedEventProducer1;
        this.tradeProducer = tradeProducer1;
        this.orderCancellationProducer = orderCancellationProducer1;
        this.matchingEngines = new HashMap<>();
        // Create a single-threaded executor for Kafka listeners
        this.kafkaListenerExecutor = Executors.newSingleThreadExecutor();
    }

    /**
     * Initializes the MatchingService after all dependencies have been injected.
     * This method orchestrates the initial loading of open orders and starting
     * of real-time Kafka listeners.
     */
    @PostConstruct
    public void init() {
        log.info("[MatchingService] Initializing engines..");
        initializeMatchingEngines();
        log.info("[MatchingService] Starting kafka listener for order commands");
        kafkaListenerExecutor.submit(() -> {
            try {
                startKafkaListeners();
            } catch (Exception e) {
                log.error("[MatchingService] Error during initialization: {}", e.getMessage(), e);
            }
        });
    }

    /**
     * Shuts down the Kafka listener executor gracefully when the application stops.
     */
    @PreDestroy
    public void shutdown() {
        log.info("[MatchingService] Initiating graceful shutdown of Kafka listeners.");
        // Stop the real-time consumer first if it has a stop method
        orderCommandConsumer.stopConsuming();

        // Shutdown the executor that runs the Kafka listeners
        kafkaListenerExecutor.shutdown();
        try {
            if (!kafkaListenerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("[MatchingService] Kafka listener executor did not terminate gracefully. Forcing shutdown.");
                kafkaListenerExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("[MatchingService] Shutdown of Kafka listener executor interrupted.");
        }
        log.info("[MatchingService] Kafka listener shutdown complete.");
        log.info("[MatchingService] Shutting down MatchingEngine gracefully...");
        matchingEngines.values().forEach(MatchingEngine::shutdown);
        log.info("[MatchingService] MatchingEngine shutdown initiated.");
    }

    private void initializeMatchingEngines() {
        for (String instrumentId : orderBookManager.getInstrumentIds()) {
            OrderBook orderBook = orderBookManager.getOrderBook(instrumentId);
            if (orderBook != null) {
                // Initialize engine with its own orderbook
                MatchingEngine engine = new MatchingEngine(orderBook,
                        orderMatchedEventProducer,
                        tradeProducer,
                        orderCancellationProducer);
                matchingEngines.put(instrumentId, engine);
                log.info("[MatchingService] Initialized MatchingEngine for instrument: {}", instrumentId);
            } else {
                log.error("[MatchingService] Could not find OrderBook for instrument: {}. Skipping engine initialization.", instrumentId);
            }
        }
    }


    /**
     * Starts listening to real-time Kafka topics for new orders and order updates.
     * The `handleNewOrUpdatedOrderEvent` method will process each incoming event.
     */
    private void startKafkaListeners() {
        log.info("[MatchingService] Starting real-time Kafka listeners for order events.");
        orderCommandConsumer.startConsuming("order.events", this::handleOrderCommand);
    }

    /**
     * Processes a new or updated order event received from real-time Kafka stream.
     * This method decides whether the order needs to be added to the matching queue.
     * @param eventMessage The order object received from Kafka.
     */
    private void handleOrderCommand(OrderCommandEvent eventMessage) {
        Order eventOrder = eventMessage.getOrder(); // Get Order from msg
        String eventType = eventMessage.getEventType(); // Get EventType from msg
        String instrumentId = eventOrder.getInstrumentId();

        MatchingEngine engine = matchingEngines.get(instrumentId);

        log.info("[MatchingService] Received real-time event from Kafka: EventType={}, OrderId={}",
                eventType, eventOrder.getOrderId());

        if ("OrderCreated".equals(eventType) || ("OrderUpdated".equals(eventType) && eventOrder.getStatus() == OrderStatus.OPEN)) {
            engine.addToOrderQueue(eventOrder);
            log.info("[MatchingService] Enqueued new order {} for matching.", eventOrder.getOrderId());
        } else if ("OrderCancelled".equals(eventType)) {
            log.info("[MatchingService] Received cancellation for order {}. Attempting to remove from book.", eventOrder.getOrderId());
            Order ordertoCancel = engine.getOpenOrderById(eventOrder.getOrderId());
            if (ordertoCancel != null) {
                engine.cancelOrder(eventOrder.getOrderId());
                log.info("[MatchingService] Order {} successfully cancelled in OrderBook.", eventOrder.getOrderId());
            } else {
                log.info("[MatchingService] Order {} not found as OPEN in OrderBook. No action needed for cancellation.", eventOrder.getOrderId());
            }
        } else {
            log.debug("[MatchingService] EventType={} for order {} has status {}. Not enqueuing or cancelling (likely internal or already processed).",
                    eventType, eventOrder.getOrderId(), eventOrder.getStatus());
        }
    }

}