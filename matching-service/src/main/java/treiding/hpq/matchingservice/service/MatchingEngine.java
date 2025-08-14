package treiding.hpq.matchingservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafkaevent.orderevent.CancellationReason;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderConfirmedCancellationEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderMatchedEvent;
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
                          OrderMatchedEventProducer orderMatchedEventProducer,
                          TradeEventProducer tradeProducer, OrderCancellationProducer orderCancellationProducer) {
        this.orderBook = orderBook;
        this.orderCancellationProducer = orderCancellationProducer;

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
                try {
                    Order newOrder = incomingOrderQueue.poll();
                    if (newOrder != null) {
                        log.info("[MatchingEngine] Processing order from queue: {}", newOrder.getOrderId());
                        processOrderInternal(newOrder);
                    } else {
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("[MatchingEngine] Matching thread interrupted, shutting down: {}", e.getMessage());
                    isRunning.set(false);
                } catch (Exception e) {
                    log.error("[MatchingEngine] Error processing order in matching thread: {}", e.getMessage(), e);
                }
            }
            log.info("[MatchingEngine] Matching thread stopped gracefully.");
        });
    }

    /**
     * Shuts down the matching engine gracefully.
     */
    public void shutdown() {
        log.info("[MatchingEngine] Initiating shutdown...");
        isRunning.set(false);
        matchingExecutor.shutdown();
        try {
            if (!matchingExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.error("[MatchingEngine] Matching engine did not terminate gracefully within 30s. Forcing shutdown.");
                matchingExecutor.shutdownNow();
                if (!matchingExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("[MatchingEngine] Matching engine did not terminate after forceful shutdown.");
                }
            }
        } catch (InterruptedException ie) {
            matchingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("[MatchingEngine] Shutdown interrupted.");
        }
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
     * @param newOrder The new order (incoming aggressor order) to process.
     */
    private void processOrderInternal(Order newOrder) {
        log.info("[MatchingEngine] Matching order: {} {} {} at {}",
                newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());

        // Fetch the latest order state from the database via OrderBook
        Order currentOrderState = orderBook.getOrderDetail(newOrder.getOrderId());
        if (currentOrderState == null || currentOrderState.getStatus() == OrderStatus.CANCELED || currentOrderState.getStatus() == OrderStatus.FILLED) {
            log.warn("[MatchingEngine] Order {} (current status: {}) is no longer eligible for matching. Skipping.",
                    newOrder.getOrderId(), currentOrderState != null ? currentOrderState.getStatus() : "null/removed");
            return;
        }

        if (newOrder.getSide() == OrderSide.BUY) {
            processBuyOrder(currentOrderState);
        } else {
            processSellOrder(currentOrderState);
        }
    }

    /**
     * Processes an incoming BUY order against existing SELL orders in the order book.
     *
     * @param newOrder The incoming buy order.
     */
    private void processBuyOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> askIter = orderBook.getAskLevelsIterator(newOrder.getInstrumentId());
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && askIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> askLevel = askIter.next();
            BigDecimal askPrice = askLevel.getKey();
            List<Order> restingAsks = askLevel.getValue();

            if (newOrder.getPrice().compareTo(askPrice) >= 0) {
                log.debug("  Attempting to match buy order {} with ask level at price {}", newOrder.getOrderId(), askPrice);
                processLevel(newOrder, restingAsks, askPrice, OrderSide.BUY);
            } else {
                log.debug("  Buy order {} price {} is lower than best ask price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), askPrice);
                break;
            }
        }
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0) {
            orderBook.addOrder(newOrder);
            log.info("  New buy order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
        }
    }

    /**
     * Processes an incoming SELL order against existing BUY orders in the order book.
     * @param newOrder The incoming sell order.
     */
    private void processSellOrder(Order newOrder) {
        Iterator<Map.Entry<BigDecimal, List<Order>>> bidIter = orderBook.getBidLevelsIterator(newOrder.getInstrumentId());
        while (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && bidIter.hasNext()) {
            Map.Entry<BigDecimal, List<Order>> bidLevel = bidIter.next();
            BigDecimal bidPrice = bidLevel.getKey();
            List<Order> restingBids = bidLevel.getValue();

            if (newOrder.getPrice().compareTo(bidPrice) <= 0) {
                log.debug("  Attempting to match sell order {} with bid level at price {}", newOrder.getOrderId(), bidPrice);
                processLevel(newOrder, restingBids, bidPrice, OrderSide.SELL);
            } else {
                log.debug("  Sell order {} price {} is higher than best bid price {}, breaking matching.", newOrder.getOrderId(), newOrder.getPrice(), bidPrice);
                break;
            }
        }
        if (newOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0) {
            orderBook.addOrder(newOrder);
            log.info("  New sell order {} has remaining quantity {} and added as resting order.", newOrder.getOrderId(), newOrder.getRemainingQuantity());
        }
    }

    /**
     * Processes the matching logic at a specific price level.
     */
    private void processLevel(Order aggressorOrder, List<Order> restingOrders, BigDecimal tradePrice, OrderSide aggressorSide) {
        Iterator<Order> restingIter = restingOrders.iterator();
        while (aggressorOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && restingIter.hasNext()) {
            Order restingOrder = restingIter.next();

            Order currentRestingOrderState = orderBook.getOrderDetail(restingOrder.getOrderId());
            if (currentRestingOrderState == null || currentRestingOrderState.getStatus() == OrderStatus.CANCELED || currentRestingOrderState.getStatus() == OrderStatus.FILLED) {
                log.debug("  Resting order {} (current status: {}) is no longer eligible for matching. Removing from level.",
                        restingOrder.getOrderId(), currentRestingOrderState != null ? currentRestingOrderState.getStatus() : "null/removed");
                restingIter.remove();
                continue;
            }
            restingOrder = currentRestingOrderState;

            BigDecimal tradeQuantity = aggressorOrder.getRemainingQuantity().min(restingOrder.getRemainingQuantity());

            if (tradeQuantity.compareTo(BigDecimal.ZERO) > 0) {
                Trade trade = createTrade(aggressorOrder, restingOrder, tradePrice, tradeQuantity, aggressorSide);
                notifyTradeListeners(trade);
                log.debug("  Trade created and sent to Kafka: ID={}, quantity={}, price={}", trade.getTradeId(), tradeQuantity, tradePrice);

                aggressorOrder.fill(tradeQuantity);
                restingOrder.fill(tradeQuantity);

                notifyOrderMatchedListeners(trade.getTradeId(), aggressorOrder, restingOrder, tradeQuantity, tradePrice, aggressorSide);

                log.info("  Matched {} at {} (Aggressor Order: {} left, Resting Order: {} left)",
                        tradeQuantity, tradePrice, aggressorOrder.getRemainingQuantity(), restingOrder.getRemainingQuantity());

                orderBook.updateOrder(aggressorOrder);
                orderBook.updateOrder(restingOrder);

                if (restingOrder.getStatus() == OrderStatus.FILLED) {
                    restingIter.remove();
                    log.info("  Resting order {} FILLED and removed from order book.", restingOrder.getOrderId());
                }
            }
        }
    }

    /**
     * Creates a Trade object based on the matched orders and trade details.
     */
    private Trade createTrade(Order aggressorOrder, Order restingOrder, BigDecimal tradePrice, BigDecimal tradeQuantity, OrderSide aggressorSide) {
        String buyerOrderId = (aggressorSide == OrderSide.BUY) ? aggressorOrder.getOrderId() : restingOrder.getOrderId();
        String sellerOrderId = (aggressorSide == OrderSide.BUY) ? restingOrder.getOrderId() : aggressorOrder.getOrderId();
        String buyerId = (aggressorSide == OrderSide.BUY) ? aggressorOrder.getUserId() : restingOrder.getUserId();
        String sellerId = (aggressorSide == OrderSide.BUY) ? restingOrder.getUserId() : aggressorOrder.getUserId();
        String instrumentId = aggressorOrder.getInstrumentId();

        return new Trade(
                UUID.randomUUID().toString(),
                buyerOrderId,
                buyerId,
                sellerOrderId,
                sellerId,
                instrumentId,
                tradePrice,
                tradeQuantity,
                Instant.now()
        );
    }

    /**
     * Enqueues a new incoming order for asynchronous processing by the single matching thread.
     */
    public void addToOrderQueue(Order newOrder) {
        orderBook.addOrder(newOrder);
        incomingOrderQueue.offer(newOrder);
        log.info("[MatchingEngine] Enqueued new order: {} (Side: {}, Qty: {}, Price: {})",
                newOrder.getOrderId(), newOrder.getSide(), newOrder.getOriginalQuantity(), newOrder.getPrice());
    }

    /**
     * Adds a resting order directly to the order book.
     */
    public void addRestingOrder(Order order) {
        if (order.getRemainingQuantity().compareTo(BigDecimal.ZERO) <= 0 ||
                (order.getStatus() != OrderStatus.OPEN && order.getStatus() != OrderStatus.PARTIALLY_FILLED)) {
            log.warn("[MatchingEngine] Attempted to add invalid resting order: {} (Qty: {}, Status: {}). Skipping.",
                    order.getOrderId(), order.getRemainingQuantity(), order.getStatus());
            return;
        }
        orderBook.addOrder(order);
        log.info("[MatchingEngine] Added resting order: {} {} {}@{}",
                order.getOrderId(), order.getSide(), order.getRemainingQuantity(), order.getPrice());
    }

    /**
     * Cancels an order by its ID.
     * The cancellation request is enqueued to be processed by the matching thread.
     */
    public void cancelOrder(String orderId) {
        ancillaryTaskExecutor.submit(() -> {
            CancellationReason reason = CancellationReason.UNKNOWN_REASON;
            Order orderToCancel = orderBook.getOrderDetail(orderId);

            if (orderToCancel != null &&
                    (orderToCancel.getStatus() == OrderStatus.OPEN || orderToCancel.getStatus() == OrderStatus.PARTIALLY_FILLED)) {
                orderBook.removeOrder(orderToCancel);
                orderToCancel.setStatus(OrderStatus.CANCELED);

                orderCancellationProducer.sendConfirmedCancellationEvent(new OrderConfirmedCancellationEvent(orderToCancel.getOrderId(), "CANCELED_BY_REQUEST"));

                log.info("[MatchingEngine] Order {} has been CANCELED. Status update sent to Order Service.", orderId);

            } else {
                if (orderToCancel == null) {
                    reason = CancellationReason.ORDER_NOT_FOUND;
                    log.warn("[MatchingEngine] Order {} not found. Ignoring cancellation request.", orderId);
                    orderCancellationProducer.sendRejectedCancellationEvent(new OrderCancellationRejectedEvent(orderId, reason));
                } else {
                    if (orderToCancel.getStatus() == OrderStatus.CANCELED) {
                        reason = CancellationReason.ORDER_ALREADY_CANCELED;
                    } else if (orderToCancel.getStatus() == OrderStatus.FILLED) {
                        reason = CancellationReason.ORDER_ALREADY_FILLED;
                    } else {
                        reason = CancellationReason.INVALID_ORDER_STATE;
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
     */
    private void notifyTradeListeners(Trade trade) {
        tradeProducer.sendTradeEvent(trade);
    }

    /**
     * Creates and sends an OrderMatchedEvent to Kafka for both orders involved in a trade.
     */
    private void notifyOrderMatchedListeners(String tradeId, Order aggressorOrder, Order restingOrder,
                                             BigDecimal matchedQuantity, BigDecimal price, OrderSide aggressorSide) {

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
     */
    public Order getOpenOrderById(String orderId) {
        return orderBook.getOrderDetail(orderId);
    }

    /**
     * Resets the order book, clearing all in-memory data.
     */
    public void reset() {
        log.warn("[MatchingEngine] Resetting Order Book.");
        incomingOrderQueue.clear();
        orderBook.clear();
        log.warn("[MatchingEngine] Order Book data cleared.");
    }
}