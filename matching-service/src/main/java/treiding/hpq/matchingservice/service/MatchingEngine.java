package treiding.hpq.matchingservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafkaevent.CancellationReason;
import treiding.hpq.basedomain.kafkaevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.OrderConfirmedCancellationEvent;
import treiding.hpq.basedomain.kafkaevent.OrderMatchedEvent;
import treiding.hpq.matchingservice.entity.OrderBook;
import treiding.hpq.matchingservice.kafka.OrderCancellationProducer;
import treiding.hpq.matchingservice.kafka.OrderMatchedEventProducer;
import treiding.hpq.matchingservice.kafka.TradeEventProducer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MatchingEngine {

    private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final OrderBook orderBook;
    private final Map<String, Order> allOpenOrders; // To quickly access and manage order states

    // Kafka Producers to communicate changes back to other services
    private final OrderMatchedEventProducer orderMatchedEventProducer;
    private final TradeEventProducer tradeProducer;
    private final OrderCancellationProducer orderCancellationProducer;


    // Fields for multi-threading
    private final ExecutorService matchingExecutor; // For sequential matching all orders
    private final ExecutorService ancillaryTaskExecutor;
    private final ConcurrentLinkedQueue<Order> incomingOrderQueue; // Queue for new orders
    private final AtomicBoolean isRunning; // To control the matching loop


    // Constructor now takes Kafka Producers as dependencies
    public MatchingEngine(OrderBook orderBook,
                          OrderMatchedEventProducer orderMatchedEventProducer, // New producer for matched events
                          TradeEventProducer tradeProducer, OrderCancellationProducer orderCancellationProducer) { // General status update producer
        this.orderBook = orderBook;
        this.orderCancellationProducer = orderCancellationProducer;
        this.allOpenOrders = new ConcurrentHashMap<>();

        this.orderMatchedEventProducer = orderMatchedEventProducer;
        this.tradeProducer = tradeProducer;

        // Use a single-threaded executor for sequential processing of orders
        this.matchingExecutor = Executors.newSingleThreadExecutor();
        this.ancillaryTaskExecutor = Executors.newSingleThreadExecutor();
        this.incomingOrderQueue = new ConcurrentLinkedQueue<>();
        this.isRunning = new AtomicBoolean(true); // Start the engine as running
        startMatchingThread(); // Start the thread that processes the queue
    }

    /**
     * Starts the dedicated matching thread that processes orders from the queue.
     * This ensures all order processing (new orders, cancellations, matches) is sequential.
     */
    private void startMatchingThread() {
        matchingExecutor.submit(() -> {
            log.info("[MatchingEngine] Matching thread started.");
            while (isRunning.get() || !incomingOrderQueue.isEmpty()) {
                // Keep running if active or queue has orders
                try {
                    Order newOrder = incomingOrderQueue.poll();
                    if (newOrder != null) {
                        log.info("[MatchingEngine] Processing order from queue: {}", newOrder.getOrderId());
                        // Ensure the order is tracked in allOpenOrders if it's new
                        allOpenOrders.put(newOrder.getOrderId(), newOrder);
                        processOrderInternal(newOrder);
                    } else {
                        // If no orders, sleep briefly to avoid busy-waiting and consume CPU unnecessarily
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    log.error("[MatchingEngine] Matching thread interrupted, shutting down: {}", e.getMessage());
                    isRunning.set(false); // Signal to stop processing
                } catch (Exception e) {
                    log.error("[MatchingEngine] Error processing order in matching thread: {}", e.getMessage(), e);
                }
            }
            log.info("[MatchingEngine] Matching thread stopped gracefully.");
        });
    }

    /**
     * Shuts down the matching engine gracefully.
     * This method should be called when the application is shutting down to ensure all pending
     * orders in the queue are processed and resources are released.
     */
    public void shutdown() {
        log.info("[MatchingEngine] Initiating shutdown...");
        // Shut down matching executor
        isRunning.set(false); // Signal the matching thread to stop after processing current queue
        matchingExecutor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks (orders in queue) to terminate
            if (!matchingExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.error("[MatchingEngine] Matching engine did not terminate gracefully within 30s. Forcing shutdown.");
                matchingExecutor.shutdownNow(); // Forcefully terminate if it takes too long
                // Wait a bit more for tasks to respond to being cancelled
                if (!matchingExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("[MatchingEngine] Matching engine did not terminate after forceful shutdown.");
                }
            }
            log.info("[MatchingEngine] Matching engine shutdown complete.");
        } catch (InterruptedException ie) {
            matchingExecutor.shutdownNow();
            Thread.currentThread().interrupt(); // Preserve interrupt status
            log.error("[MatchingEngine] Shutdown interrupted.");
        }
        // shut down ancillary tasks executor
        ancillaryTaskExecutor.shutdown();
        try {
            if (!ancillaryTaskExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.error("[MatchingEngine] Ancillary tasks executor did not terminate gracefully within 30s. Forcing shutdown.");
                ancillaryTaskExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            ancillaryTaskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("[MatchingEngine] Matching engine shutdown complete.");
    }

    /**
     * Internal method to process an order, called ONLY by the single matching thread.
     * This method orchestrates the matching logic for buy and sell orders.
     * @param newOrder The new order (incoming aggressor order) to process.
     */
    private void processOrderInternal(Order newOrder) {
        log.info("[MatchingEngine] Matching order: {} {} {} at {}",
                newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());

        // Ensure the order being processed is still considered OPEN or PARTIALLY_FILLED.
        // A cancellation might have been enqueued before this order got processed.
        Order currentOrderState = allOpenOrders.get(newOrder.getOrderId());
        if (currentOrderState == null || currentOrderState.getStatus() == OrderStatus.CANCELED || currentOrderState.getStatus() == OrderStatus.FILLED) {
            log.warn("[MatchingEngine] Order {} (current status: {}) is no longer eligible for matching. Skipping.",
                    newOrder.getOrderId(), currentOrderState != null ? currentOrderState.getStatus() : "null/removed");
            return;
        }

        // Use the actual state from allOpenOrders for processing, as it reflects updates from other enqueued tasks.
        newOrder = currentOrderState;

        if (newOrder.getSide() == OrderSide.BUY) {
            processBuyOrder(newOrder);
        } else {
            processSellOrder(newOrder);
        }
    }

    /**
     * Processes an incoming BUY order against existing SELL orders in the order book.
     *
     * @param newOrder The incoming buy order.
     */
    private void processBuyOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> askIter = orderBook.getAskLevelsIterator();
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && askIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> askLevel = askIter.next();
            BigDecimal askPrice = askLevel.getKey();
            List<Order> restingAsks = askLevel.getValue();

            // Match if buy order's price is greater than or equal to the ask price
            if (newOrder.getPrice().compareTo(askPrice) >= 0) {
                log.debug("  Attempting to match buy order {} with ask level at price {}", newOrder.getOrderId(), askPrice);
                // Call processLevel to handle the actual trade execution and order updates
                processLevel(newOrder, restingAsks, askPrice, OrderSide.BUY);

                // If the entire price level is depleted, remove it from the order book
                if (restingAsks.isEmpty()) {
                    askIter.remove();
                    log.debug("  Ask level at price {} is now empty and removed.", askPrice);
                }
            } else {
                log.debug("  Buy order {} price {} is lower than best ask price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), askPrice);
                break; // No more matching opportunities at higher prices
            }
        }
        // If the new order has remaining quantity after matching, add it to the order book as a resting order
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0) {
            addRestingOrder(newOrder); // Add remaining quantity as a resting order to OrderBook
            log.info("  New buy order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
        }
        // No explicit Kafka update here. If it was fully filled, updates were sent within processLevel.
    }

    /**
     * Processes an incoming SELL order against existing BUY orders in the order book.
     * @param newOrder The incoming sell order.
     */
    private void processSellOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> bidIter = orderBook.getBidLevelsIterator();
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && bidIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> bidLevel = bidIter.next();
            BigDecimal bidPrice = bidLevel.getKey();
            List<Order> restingBids = bidLevel.getValue();

            // Match if sell order's price is less than or equal to the bid price
            if (newOrder.getPrice().compareTo(bidPrice) <= 0) {
                log.debug("  Attempting to match sell order {} with bid level at price {}", newOrder.getOrderId(), bidPrice);
                // Call processLevel to handle the actual trade execution and order updates
                processLevel(newOrder, restingBids, bidPrice, OrderSide.SELL);

                // If the entire price level is depleted, remove it from the order book
                if (restingBids.isEmpty()) {
                    bidIter.remove();
                    log.debug("  Bid level at price {} is now empty and removed.", bidPrice);
                }
            } else {
                log.debug("  Sell order {} price {} is higher than best bid price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), bidPrice);
                break; // No more matching opportunities at lower prices
            }
        }
        // If the new order has remaining quantity after matching, add it to the order book as a resting order
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0) {
            addRestingOrder(newOrder); // Add remaining quantity as a resting order to OrderBook
            log.info("  New sell order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
        }
        // No explicit Kafka update here. If it was fully filled, updates were sent within processLevel.
    }

    /**
     * Processes the matching logic at a specific price level.
     * This method iterates through resting orders at a given level and executes trades.
     * It is responsible for updating order quantities and statuses, and sending Kafka events for trades and order matches.
     *
     * @param aggressorOrder The active/incoming order that is attempting to match.
     * @param restingOrders  The list of passive/resting orders at the current price level.
     * @param tradePrice     The price at which the trade occurs.
     * @param aggressorSide  The side (BUY/SELL) of the aggressor order.
     */
    private void processLevel(Order aggressorOrder, List<Order> restingOrders, BigDecimal tradePrice, OrderSide aggressorSide) {
        Iterator<Order> restingIter = restingOrders.iterator();
        while (aggressorOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && restingIter.hasNext()) {
            Order restingOrder = restingIter.next();

            // Ensure the resting order is still valid (not canceled or already fully filled)
            // This check is crucial because a cancellation might have been enqueued and processed by
            // the single matching thread *before* this specific resting order was reached in the loop.
            Order currentRestingOrderState = allOpenOrders.get(restingOrder.getOrderId());
            if (currentRestingOrderState == null || currentRestingOrderState.getStatus() == OrderStatus.CANCELED || currentRestingOrderState.getStatus() == OrderStatus.FILLED) {
                log.debug("  Resting order {} (current status: {}) is no longer eligible for matching. Removing from level.",
                        restingOrder.getOrderId(), currentRestingOrderState != null ? currentRestingOrderState.getStatus() : "null/removed");
                restingIter.remove(); // Remove invalid resting order from the level list
                continue; // Skip to the next resting order
            }
            // Use the latest state of the resting order for matching
            restingOrder = currentRestingOrderState;


            BigDecimal tradeQuantity = aggressorOrder.getRemainingQuantity().min(restingOrder.getRemainingQuantity());

            if (tradeQuantity.compareTo(BigDecimal.ZERO) > 0) {
                // Create and send Trade event
                Trade trade = createTrade(aggressorOrder, restingOrder, tradePrice, tradeQuantity, aggressorSide);
                notifyTradeListeners(trade); // Send trade to Kafka
                log.debug("  Trade created and sent to Kafka: ID={}, quantity={}, price={}", trade.getTradeId(), tradeQuantity, tradePrice);

                // Update quantities for both orders
                aggressorOrder.fill(tradeQuantity);
                restingOrder.fill(tradeQuantity);

                // Send OrderMatchedEvent for both orders involved in the trade
                notifyOrderMatchedListeners(trade.getTradeId(), aggressorOrder, restingOrder, tradeQuantity, tradePrice, aggressorSide);

                log.info("  Matched {} at {} (Aggressor Order: {} left, Resting Order: {} left)",
                        tradeQuantity, tradePrice, aggressorOrder.getRemainingQuantity(), restingOrder.getRemainingQuantity());

                // If resting order is fully filled, remove it from the level and allOpenOrders map
                if (restingOrder.getStatus() == OrderStatus.FILLED) {
                    restingIter.remove();
                    allOpenOrders.remove(restingOrder.getOrderId()); // Remove from global map
                    log.info("  Resting order {} FILLED and removed from order book.", restingOrder.getOrderId());
                }
                // If aggressorOrder is now filled, it will not be added as a resting order later.
            }
        }
    }

    /**
     * Creates a Trade object based on the matched orders and trade details.
     *
     * @param aggressorOrder The order that initiated the trade.
     * @param restingOrder   The order that was resting in the order book.
     * @param tradePrice     The price at which the trade occurred.
     * @param tradeQuantity  The quantity that was traded.
     * @param aggressorSide  The side (BUY/SELL) of the aggressor order.
     * @return A newly created Trade object.
     */
    private Trade createTrade(Order aggressorOrder, Order restingOrder, BigDecimal tradePrice, BigDecimal tradeQuantity, OrderSide aggressorSide) {
        String buyerOrderId = (aggressorSide == OrderSide.BUY) ? aggressorOrder.getOrderId() : restingOrder.getOrderId();
        String sellerOrderId = (aggressorSide == OrderSide.BUY) ? restingOrder.getOrderId() : aggressorOrder.getOrderId();
        String instrumentId = aggressorOrder.getInstrumentId(); // Both orders must be for the same instrument

        return new Trade(
                UUID.randomUUID().toString(), // Generate a unique Trade ID
                buyerOrderId,
                sellerOrderId,
                instrumentId,
                tradePrice,
                tradeQuantity,
                Instant.now() // Timestamp for the trade
        );
    }

    /**
     * Enqueues a new incoming order for asynchronous processing by the single matching thread.
     * This method is the entry point for new orders coming into the matching engine.
     * @param newOrder The new order to process.
     */
    public void addToOrderQueue(Order newOrder) {
        // kafka msg may be duplicated, so check if newOrder's already in Queue or in OrderBook
        if (!allOpenOrders.containsKey(newOrder.getOrderId())) {
            // Add the order to the queue. The matching thread will pick it up sequentially.
            incomingOrderQueue.offer(newOrder);
            // Add to allOpenOrders map immediately, so other internal methods (like cancelOrder)
            // can reference it, and the processing thread will then update its state.
            allOpenOrders.put(newOrder.getOrderId(), newOrder);
            log.info("[MatchingEngine] Enqueued new order: {} (Side: {}, Qty: {}, Price: {})",
                    newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());
        } else {
            log.info("[MatchingEngine] Order has orderId: {} already added, do nothing", newOrder.getOrderId());
            return;
        }

    }

    /**
     * Adds a resting order directly to the order book. This is typically used for initial loading of orders
     * from a persistent store (e.g., database via Order Service) on Matching Engine startup.
     * These orders are NOT re-processed for matching; they are simply restored to the book.
     * @param order The order to add. Must have positive remaining quantity and be OPEN or PARTIALLY_FILLED.
     */
    public void addRestingOrder(Order order) {
        // Kafka msg may be duplicated, check if it's already added
        if (!allOpenOrders.containsKey(order.getOrderId())) {
            if (order.getRemainingQuantity().compareTo(BigDecimal.ZERO) <= 0 ||
                    (order.getStatus() != OrderStatus.OPEN && order.getStatus() != OrderStatus.PARTIALLY_FILLED)) {
                log.warn("[MatchingEngine] Attempted to add invalid resting order: {} (Qty: {}, Status: {}). Skipping.",
                        order.getOrderId(), order.getRemainingQuantity(), order.getStatus());
                return;
            }
            orderBook.addOrder(order);
            allOpenOrders.put(order.getOrderId(), order);
            log.info("[MatchingEngine] Added resting order: {} {} {}@{}",
                    order.getOrderId(), order.getSide(), order.getRemainingQuantity(), order.getPrice());
        } else {
            log.info("[MatchingEngine] Order has OrderId: {} already added, do nothing", order.getOrderId());
            return;
        }

    }

    /**
     * Cancels an order by its ID. The cancellation request is enqueued to be processed by the matching thread.
     * This ensures thread-safety and proper ordering with other matching operations.
     * The status update (CANCELED) is then sent back to Order Service.
     * @param orderId The ID of the order to cancel.
     */
    public void cancelOrder(String orderId) {
        ancillaryTaskExecutor.submit(() -> {
            CancellationReason reason = CancellationReason.UNKNOWN_REASON; // Default reason, will be updated
            Order orderToCancel = allOpenOrders.get(orderId);
            if (orderToCancel != null) {
                log.info("[MatchingEngine] Current status of order {} for cancellation: {}", orderId, orderToCancel.getStatus());
            } else {
                log.warn("[MatchingEngine] Order {} is NULL when cancellation task runs.", orderId);
            }
            if (orderToCancel != null &&
                    (orderToCancel.getStatus() == OrderStatus.OPEN || orderToCancel.getStatus() == OrderStatus.PARTIALLY_FILLED)) {
                // Order can be canceled
                orderBook.removeOrder(orderToCancel); // Remove from the actual order book
                orderToCancel.setStatus(OrderStatus.CANCELED); // Update in-memory status
                allOpenOrders.remove(orderId); // Remove from the map of active open orders

                // Send Kafka message for confirmed cancellation
                orderCancellationProducer.sendConfirmedCancellationEvent(new OrderConfirmedCancellationEvent(orderToCancel.getOrderId(), "CANCELED_BY_REQUEST"));

                log.info("[MatchingEngine] Order {} has been CANCELED by matching thread. Status update sent to Order Service.", orderId);

            } else {
                // Order not found or cannot be canceled
                if (orderToCancel == null) {
                    reason = CancellationReason.ORDER_NOT_FOUND;
                    log.warn("[MatchingEngine] Order {} not found. Ignoring cancellation request.", orderId);
                    orderCancellationProducer.sendRejectedCancellationEvent(new OrderCancellationRejectedEvent(orderId, reason));

                } else {
                    // Order exists but is in a non-cancellable state
                    if (orderToCancel.getStatus() == OrderStatus.CANCELED) {
                        reason = CancellationReason.ORDER_ALREADY_CANCELED;
                    } else if (orderToCancel.getStatus() == OrderStatus.FILLED) {
                        reason = CancellationReason.ORDER_ALREADY_FILLED;
                    } else {
                        reason = CancellationReason.INVALID_ORDER_STATE; // Catch any other unexpected states
                    }
                    orderCancellationProducer.sendRejectedCancellationEvent(new OrderCancellationRejectedEvent(orderToCancel.getOrderId(), reason));
                    log.warn("[MatchingEngine] Order {} cannot be canceled (current status: {}). Ignoring cancellation request.",
                            orderId, orderToCancel.getStatus());
                }
            }
        });
        log.info("[MatchingEngine] Cancellation request for order {} enqueued.", orderId);
    }

    /**
     * Sends a Trade event to Kafka.
     * @param trade The Trade object to send.
     */
    private void notifyTradeListeners(Trade trade) {
        tradeProducer.sendTradeEvent(trade);
    }

    /**
     * Creates and sends an OrderMatchedEvent to Kafka for both aggressor and resting orders involved in a trade.
     * This event informs Order Service about the fill and triggers updates.
     *
     * @param tradeId         The unique ID of the trade.
     * @param aggressorOrder  The order that initiated the trade.
     * @param restingOrder    The order that was resting in the order book.
     * @param matchedQuantity The quantity that was traded.
     * @param price           The price at which the trade occurred.
     * @param aggressorSide   The side (BUY/SELL) of the aggressor order.
     */
    private void notifyOrderMatchedListeners(String tradeId, Order aggressorOrder, Order restingOrder,
                                             BigDecimal matchedQuantity, BigDecimal price, OrderSide aggressorSide) {

        // Both orders are part of the same match event.
        // The OrderMatchedEvent captures all information needed by OrderStatusUpdateService.
        OrderMatchedEvent event = new OrderMatchedEvent(
                tradeId,
                aggressorOrder.getOrderId(),
                restingOrder.getOrderId(),
                matchedQuantity,
                price,
                Instant.now()
        );
        orderMatchedEventProducer.sendOrderStatusUpdate(event);
        log.debug("Sent OrderMatchedEvent for trade {} involving {} and {} to Kafka.",
                tradeId, aggressorOrder.getOrderId(), restingOrder.getOrderId());
    }

    /**
     * Retrieves an open order by its ID from the in-memory order map.
     * This provides a quick lookup for orders currently active in the matching engine.
     *
     * @param orderId The ID of the order to retrieve.
     * @return The Order object if found and still active, otherwise null.
     */
    public Order getOpenOrderById(String orderId) {
        Order order = allOpenOrders.get(orderId);
        if (order != null) {
            log.debug("[MatchingEngine] Order {} found in in-memory map.", orderId);
            return order;
        }
        log.debug("[MatchingEngine] Order {} not found in in-memory map.", orderId);
        return null;
    }

    /**
     * Resets the order book, clearing all in-memory data.
     * This method is typically used for testing or to completely clear the engine's state.
     * It does NOT affect any persistent storage.
     */
    public void reset() {
        log.warn("[MatchingEngine] Resetting Order Book (clearing in-memory data).");
        incomingOrderQueue.clear();
        orderBook.clear();
        allOpenOrders.clear();
        log.warn("[MatchingEngine] In-memory Order Book data cleared.");
    }
}