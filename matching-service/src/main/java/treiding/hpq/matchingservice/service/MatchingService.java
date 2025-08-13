package treiding.hpq.matchingservice.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCommandEvent;
import treiding.hpq.matchingservice.entity.OrderBook;
import treiding.hpq.matchingservice.kafka.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Service
public class MatchingService {

    private static final Logger log = LoggerFactory.getLogger(MatchingService.class);

    private final OrderBook orderBook;
    private final MatchingEngine matchingEngine;
    private final OrderInitialLoadConsumer orderInitialLoadConsumer;
    private final OrderCommandConsumer orderCommandConsumer; // Consumer for real-time order events

    // A dedicated executor for Kafka listeners to run in the background
    // This allows the main application thread to continue starting up.
    private final ExecutorService kafkaListenerExecutor;


    public MatchingService(OrderInitialLoadConsumer orderInitialLoadConsumer,
                           OrderCommandConsumer orderCommandConsumer,
                           OrderMatchedEventProducer orderMatchedEventProducer,
                           TradeEventProducer tradeProducer,
                           OrderCancellationProducer orderCancellationProducer) {
        this.orderBook = new OrderBook();
        this.matchingEngine = new MatchingEngine(this.orderBook, orderMatchedEventProducer, tradeProducer, orderCancellationProducer);
        this.orderInitialLoadConsumer = orderInitialLoadConsumer;
        this.orderCommandConsumer = orderCommandConsumer; // Initialize the real-time consumer

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
        log.info("[MatchingService] Initializing. Loading open orders and starting Kafka listeners.");
        // We'll run initial load and listener startup in a separate thread
        // to avoid blocking the main application startup, especially if Kafka takes time.
        kafkaListenerExecutor.submit(() -> {
            try {
                loadOpenOrdersIntoEngineFromKafka();
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
        this.matchingEngine.shutdown();
        log.info("[MatchingService] MatchingEngine shutdown initiated.");
    }

    /**
     * Loads existing OPEN and PARTIALLY_FILLED orders from a Kafka topic
     * into the MatchingEngine's order book. This is crucial for rebuilding
     * the engine's state upon startup.
     */
    private void loadOpenOrdersIntoEngineFromKafka() {
        log.info("[MatchingService] Loading open orders from Kafka for initial engine state...");
        List<Order> openOrders = new ArrayList<>();

        // Consume all messages from the dedicated initial load topic
        List<Order> ordersFromKafka = orderInitialLoadConsumer.consumeAllOrdersUntilEndOfTopic("order.initial-load");

        for (Order order : ordersFromKafka) {
            // Only consider orders that are currently open or partially filled
            if (order.getStatus() == OrderStatus.OPEN || order.getStatus() == OrderStatus.PARTIALLY_FILLED) {
                openOrders.add(order);
                log.debug("[MatchingService] Found open/partially filled order from Kafka: {}", order.getOrderId());
            }
        }

        // Sort orders by timestamp to ensure they are added to the engine in chronological order.
        // This is important to correctly rebuild the order book state.
        openOrders.sort(Comparator.comparing(Order::getTimestamp));
        log.debug("[MatchingService] Sorted {} open orders by timestamp for loading.", openOrders.size());

        for (Order order : openOrders) {
            // Add the order directly to the MatchingEngine as a "resting order".
            // These orders are already part of the existing market state and do not need
            // to go through the active matching process. They simply restore the order book.
            matchingEngine.addRestingOrder(order);
        }
        log.info("[MatchingService] Finished loading {} open orders into MatchingEngine.", openOrders.size());
    }

    /**
     * Starts listening to real-time Kafka topics for new orders and order updates.
     * The `handleNewOrUpdatedOrderEvent` method will process each incoming event.
     */
    private void startKafkaListeners() {
        log.info("[MatchingService] Starting real-time Kafka listeners for order events.");

        orderCommandConsumer.startConsuming("order.events", this::handleNewOrUpdatedOrderEvent);

        // You might have other consumers here, e.g., for cancellation requests
        // cancelOrderConsumer.startConsuming("order.cancellations", this::handleCancellationRequest);
    }

    /**
     * Processes a new or updated order event received from real-time Kafka stream.
     * This method decides whether the order needs to be added to the matching queue.
     * @param eventMessage The order object received from Kafka.
     */
    private void handleNewOrUpdatedOrderEvent(OrderCommandEvent eventMessage) {
        Order eventOrder = eventMessage.getOrder(); // Lấy đối tượng Order từ message
        String eventType = eventMessage.getEventType(); // Lấy loại sự kiện

        log.info("[MatchingService] Received real-time event from Kafka: EventType={}, OrderId={}",
                eventType, eventOrder.getOrderId());

        if ("OrderCreated".equals(eventType) || ("OrderUpdated".equals(eventType) && eventOrder.getStatus() == OrderStatus.OPEN)) {
            // Xử lý tạo mới hoặc cập nhật trạng thái OPEN
            matchingEngine.addToOrderQueue(eventOrder);
            log.info("[MatchingService] Enqueued new order {} for matching.", eventOrder.getOrderId());
        } else if ("OrderCancelled".equals(eventType)) {
            // Xử lý hủy đơn hàng
            log.info("[MatchingService] Received cancellation for order {}. Attempting to remove from book.", eventOrder.getOrderId());
            Order ordertoCancel = matchingEngine.getOpenOrderById(eventOrder.getOrderId());
            if (ordertoCancel != null) {
                matchingEngine.cancelOrder(eventOrder.getOrderId());
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