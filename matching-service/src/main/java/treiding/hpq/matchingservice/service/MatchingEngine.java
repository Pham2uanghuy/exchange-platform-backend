package treiding.hpq.matchingservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.matchingservice.entity.OrderBook;
import treiding.hpq.matchingservice.kafka.OrderStatusUpdateProducer;
import treiding.hpq.matchingservice.kafka.TradeEventProducer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;



public class MatchingEngine {

    private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final OrderBook orderBook;
    private final Map<String, Order> allOpenOrders; // To quickly access and manage order states

    // Kafka Producers to communicate changes back to other services (e.g., Order Service)
    private final OrderStatusUpdateProducer orderStatusUpdateProducer; // NEW: To send order updates to Kafka
    private final TradeEventProducer tradeProducer;   // NEW: To send trade events to Kafka


    // Fields for multi-threading
    private final ExecutorService matchingExecutor; // For sequential matching all orders
    private final ConcurrentLinkedQueue<Order> incomingOrderQueue; // Queue for new orders
    private final AtomicBoolean isRunning; // To control the matching loop


    // Constructor now takes Kafka Producers as dependencies
    public MatchingEngine(OrderBook orderBook, OrderStatusUpdateProducer orderStatusUpdateProducer, TradeEventProducer tradeProducer) {
        this.orderBook = orderBook;
        this.allOpenOrders = new ConcurrentHashMap<>();

        this.orderStatusUpdateProducer = orderStatusUpdateProducer;
        this.tradeProducer = tradeProducer;

        // Use a single-threaded executor for sequential processing of orders
        this.matchingExecutor = Executors.newSingleThreadExecutor();
        this.incomingOrderQueue = new ConcurrentLinkedQueue<>();
        this.isRunning = new AtomicBoolean(true); // Start the engine as running
        startMatchingThread(); // Start the thread that processes the queue
    }

    /**
     * Starts the dedicated matching thread that processes orders from the queue.
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
                        // This might be redundant if addToOrderQueue already does it, but good for safety.
                        allOpenOrders.put(newOrder.getOrderId(), newOrder);
                        processOrderInternal(newOrder);
                    } else {
                        // If no orders, sleep briefly to avoid busy-waiting
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    log.error("[MatchingEngine] Matching thread interrupted, shutting down: {}", e.getMessage());
                    isRunning.set(false); // Signal to stop processing
                } catch (Exception e) {
                    log.error("[MatchingEngine] Error processing order in matching thread: {}", e.getMessage(), e); // Log full stack trace
                }
            }
            log.info("[MatchingEngine] Matching thread stopped gracefully.");
        });
    }

    /**
     * Shuts down the matching engine gracefully.
     * This method should be called when the application is shutting down.
     */
    public void shutdown() {
        log.info("[MatchingEngine] Initiating shutdown...");
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
    }

    /**
     * Internal method to process an order, called ONLY by the single matching thread.
     * @param newOrder The new order to process.
     */
    private void processOrderInternal(Order newOrder) {
        log.info("[MatchingEngine] Matching order: {} {} {} at {}",
                newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());

        if (newOrder.getSide() == OrderSide.BUY) {
            processBuyOrder(newOrder);
        } else {
            processSellOrder(newOrder);
        }
    }

    private void processBuyOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> askIter = orderBook.getAskLevelsIterator();
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.valueOf(0.0)) > 0 && askIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> askLevel = askIter.next();
            BigDecimal askPrice = askLevel.getKey();
            List<Order> restingAsks = askLevel.getValue();

            if (newOrder.getPrice().compareTo(askPrice) >= 0) {
                log.debug("  Attempting to match buy order {} with ask level at price {}", newOrder.getOrderId(), askPrice);
                processLevel(newOrder, restingAsks, askPrice, OrderSide.BUY);
                if (restingAsks.isEmpty()) {
                    askIter.remove();
                    log.debug("  Ask level at price {} is now empty and removed.", askPrice);
                }
            } else {
                log.debug("  Buy order {} price {} is lower than best ask price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), askPrice);
                break;
            }
        }
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.valueOf(0.0)) > 0) {
            addRestingOrder(newOrder); // Add remaining quantity as a resting order to OrderBook
            log.info("  New buy order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
            // No need to send Kafka update here, as addRestingOrder only adds to in-memory book.
            // The initial state was already published by Order Service.
        }
        // If the new order is fully filled, its status update would have been sent within processLevel
    }

    private void processSellOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> bidIter = orderBook.getBidLevelsIterator();
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.valueOf(0)) > 0 && bidIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> bidLevel = bidIter.next();
            BigDecimal bidPrice = bidLevel.getKey();
            List<Order> restingBids = bidLevel.getValue();

            if (newOrder.getPrice().compareTo(bidPrice) <= 0) {
                log.debug("  Attempting to match sell order {} with bid level at price {}", newOrder.getOrderId(), bidPrice);
                processLevel(newOrder, restingBids, bidPrice, OrderSide.SELL);
                if (restingBids.isEmpty()) {
                    bidIter.remove();
                    log.debug("  Bid level at price {} is now empty and removed.", bidPrice);
                }
            } else {
                log.debug("  Sell order {} price {} is higher than best bid price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), bidPrice);
                break;
            }
        }
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.valueOf(0.0)) > 0) {
            addRestingOrder(newOrder); // Add remaining quantity as a resting order to OrderBook
            log.info("  New sell order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
            // No need to send Kafka update here.
        }
    }

    private void processLevel(Order newOrder, List<Order> restingOrders, BigDecimal tradePrice, OrderSide side) {
        synchronized (restingOrders) { // Still good practice to synchronize on the list for thread safety within the level
            Iterator<Order> restingIter = restingOrders.iterator();
            while (newOrder.getRemainingQuantity().compareTo(BigDecimal.valueOf(0.0)) > 0 && restingIter.hasNext()) {
                Order restingOrder = restingIter.next();
                BigDecimal tradeQuantity = newOrder.getRemainingQuantity().min(restingOrder.getRemainingQuantity());

                if (tradeQuantity.compareTo(BigDecimal.valueOf(0.0)) > 0) {
                    Trade trade = createTrade(newOrder, restingOrder, tradePrice, tradeQuantity, side);
                    notifyTradeListeners(trade); // Send trade to Kafka
                    log.debug("  Trade created and sent to Kafka: ID={}, quantity={}, price={}", trade.getTradeId(), tradeQuantity, tradePrice);

                    newOrder.fill(tradeQuantity); // Update newOrder status
                    notifyOrderListeners(newOrder); //Send newOrder status update to Kafka
                    log.debug("  New order {} filled by {} quantity, remaining {}. Status update sent to Kafka.",
                            newOrder.getOrderId(), tradeQuantity, newOrder.getRemainingQuantity());


                    restingOrder.fill(tradeQuantity); // Update restingOrder status
                    notifyOrderListeners(restingOrder); // Send restingOrder status update to Kafka
                    log.debug("  Resting order {} filled by {} quantity, remaining {}. Status update sent to Kafka.",
                            restingOrder.getOrderId(), tradeQuantity, restingOrder.getRemainingQuantity());


                    log.info("  Matched {} at {} (New {} Order: {} left, Resting {}: {} left)",
                            tradeQuantity, tradePrice, side, newOrder.getRemainingQuantity(),
                            (side == OrderSide.BUY ? "Ask" : "Bid"), restingOrder.getRemainingQuantity());

                    if (restingOrder.getStatus() == OrderStatus.FILLED) {
                        restingIter.remove();
                        allOpenOrders.remove(restingOrder.getOrderId()); // Remove from global map
                        // Removed Redis deletion for filled order
                        log.info("  Resting {} {} FILLED and removed from order book.",
                                (side == OrderSide.BUY ? "Ask" : "Bid"), restingOrder.getOrderId());
                    }
                    // If newOrder is now filled, it will not be added as a resting order later in processBuy/SellOrder
                    // and its final update was already sent.
                }
            }
        }
    }

    private Trade createTrade(Order newOrder, Order restingOrder, BigDecimal tradePrice, BigDecimal tradeQuantity, OrderSide side) {
        String buyerOrderId = (side == OrderSide.BUY) ? newOrder.getOrderId() : restingOrder.getOrderId();
        String sellerOrderId = (side == OrderSide.BUY) ? restingOrder.getOrderId() : newOrder.getOrderId();
        String instrumentId = newOrder.getInstrumentId(); // Both orders must be for the same instrument

        return new Trade(
                UUID.randomUUID().toString(),
                buyerOrderId,
                sellerOrderId,
                instrumentId,
                tradePrice,
                tradeQuantity,
                Instant.now()
        );
    }

    /**
     * Enqueues a new incoming order for asynchronous processing by the single matching thread.
     * @param newOrder The new order to process.
     */
    public void addToOrderQueue(Order newOrder) {
        incomingOrderQueue.offer(newOrder);
        // Removed initial state saving to Redis
        log.info("[MatchingEngine] Enqueued new order: {} (Side: {}, Qty: {}, Price: {})",
                newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());
    }

    /**
     * Adds a resting order directly to the order book. This is typically used for loading orders from DB on startup.
     * These orders are NOT re-processed for matching; they are simply restored to the book.
     * @param order The order to add.
     */
    public void addRestingOrder(Order order) {
        if (order.getRemainingQuantity().compareTo(BigDecimal.valueOf(0.0)) <= 0 ||
                (order.getStatus() != OrderStatus.OPEN && order.getStatus() != OrderStatus.PARTIALLY_FILLED)) {
            log.warn("[MatchingEngine] Attempted to add invalid resting order: {} (Qty: {}, Status: {}).",
                    order.getOrderId(), order.getRemainingQuantity(), order.getStatus());
            return;
        }
        orderBook.addOrder(order);
        allOpenOrders.put(order.getOrderId(), order);
        log.info("[MatchingEngine] Added resting order: {} {} {}@{}",
                order.getOrderId(), order.getSide(), order.getRemainingQuantity(), order.getPrice());
    }

    /**
     * Cancels an order by its ID. The request is enqueued to be processed by the matching thread.
     * @param orderId The ID of the order to cancel.
     */
    public void cancelOrder(String orderId) {
        // Enqueue cancellation request to be processed by the single matching thread.
        // This ensures thread-safety and proper ordering with other matching operations.
        matchingExecutor.submit(() -> {
            Order orderToCancel = allOpenOrders.get(orderId); // Re-fetch to ensure latest state
            if (orderToCancel != null && orderToCancel.getStatus() != OrderStatus.FILLED && orderToCancel.getStatus() != OrderStatus.CANCELED) {
                orderBook.removeOrder(orderToCancel);
                orderToCancel.setStatus(OrderStatus.CANCELED);
                allOpenOrders.remove(orderId);
                // Update status in Redis to CANCELED, or remove it as it's no longer 'open'
                notifyOrderListeners(orderToCancel); // Notify listeners about cancellation
                log.info("[OrderBookEngine] Order {} has been CANCELED by matching thread.", orderId);
            } else {
                log.warn("[OrderBookEngine] Order {} not found or cannot be canceled (current status: {}).",
                        orderId, (orderToCancel != null ? orderToCancel.getStatus() : "null"));
            }
        });
        log.info("[OrderBookEngine] Cancellation request for order {} enqueued.", orderId);
    }


    private void notifyTradeListeners(Trade trade) {
        tradeProducer.sendTradeEvent(trade);
    }

    private void notifyOrderListeners(Order order) {
        orderStatusUpdateProducer.sendOrderStatusUpdate(order);
    }

    /**
     * Resets the order book, clearing in-memory data.
     * This no longer clears Redis data, as Redis is not directly managed by MatchingEngine.
     */
    public void reset() {
        log.warn("[MatchingEngine] Resetting Order Book (clearing in-memory data).");
        incomingOrderQueue.clear();
        orderBook.clear();
        allOpenOrders.clear();
        // Removed Redis clearing logic
        log.warn("[MatchingEngine] In-memory Order Book data cleared.");
    }
}