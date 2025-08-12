package treiding.hpq.orderstatusservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderConfirmedCancellationEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderMatchedEvent;
import treiding.hpq.orderstatusservice.repository.OrderRepository;
import treiding.hpq.orderstatusservice.repository.TradeRepository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;


@Service
public class OrderStatusUpdateService {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusUpdateService.class);

    private final OrderRepository orderRepository;
    private final TradeRepository tradeRepository;

    public OrderStatusUpdateService(OrderRepository orderRepository, TradeRepository tradeRepository) {
        this.orderRepository = orderRepository;
        this.tradeRepository = tradeRepository;
    }

    /**
     * Handles an incoming OrderMatchedEvent from Kafka.
     * This method is typically called by a Kafka consumer configured to listen to the
     * topic where MatchingEngine publishes OrderMatchedEvents.
     * It updates the statuses and quantities of both the aggressor and resting orders
     * involved in the match in the database.
     *
     * @param event The OrderMatchedEvent object received from Kafka.
     */
    @Transactional
    public void handleOrderMatchedEvent(OrderMatchedEvent event) {
        if (event == null || event.getTradeId() == null) {
            log.warn("Received null or invalid OrderMatchedEvent. Skipping processing.");
            return;
        }

        log.info("Processing OrderMatchedEvent for Trade ID: {}. Matched Qty: {}, Price: {}",
                event.getTradeId(), event.getMatchedQuantity(), event.getPrice());

        // Update the aggressor order
        updateOrderFromMatchedEvent(event.getAggressorOrderId(), event.getMatchedQuantity());

        // Update the resting order
        updateOrderFromMatchedEvent(event.getPassiveOrderId(), event.getMatchedQuantity());
    }

    /**
     * Internal helper method to update an order's status and quantities based on a matched event.
     * This avoids code duplication when updating both aggressor and resting orders.
     * @param orderId The ID of the order to update.
     * @param matchedQuantity The quantity that was matched in this trade.
     */
    private void updateOrderFromMatchedEvent(String orderId, BigDecimal matchedQuantity) {
        Optional<Order> existingOrderOptional = orderRepository.findByOrderId(orderId);

        if (existingOrderOptional.isPresent()) {
            Order existingOrder = existingOrderOptional.get();

            if (existingOrder.getStatus() == OrderStatus.FILLED) {
                log.warn("Received matched event for order {} which is already FILLED. Skipping update to prevent over-filling.",
                        orderId);
                return;
            }

            // If the order was CANCELED in DB due to a race condition (OrderCancellationEvent may precedence)
            if (existingOrder.getStatus() == OrderStatus.CANCELED) {
                log.warn("Order {} was found CANCELED in DB but received a matched event. Prioritizing fill. Changing status from CANCELED to PARTIALLY_FILLED/FILLED.",
                        orderId);
            }

            existingOrder.fill(matchedQuantity);
            existingOrder.setTimestamp(Instant.now()); // Update timestamp to reflect the update time

            try {
                orderRepository.save(existingOrder); // Optimistic locking will protect here
                log.info("Order ID: {} successfully updated to status: {} with Remaining Qty: {} in DB due to match.",
                        existingOrder.getOrderId(), existingOrder.getStatus(), existingOrder.getRemainingQuantity());

                if (existingOrder.getStatus() == OrderStatus.FILLED) {
                    log.info("Order ID: {} is now fully FILLED.", existingOrder.getOrderId());
                }
            } catch (Exception e) { // Catch OptimisticLockException or other DB errors
                log.error("Failed to save updated order {} due to DB error or optimistic locking conflict: {}", orderId, e.getMessage(), e);
                // a more sophisticated retry or reconciliation mechanism here.
            }

        } else {
            log.error("Order ID: {} not found in database during OrderMatchedEvent processing. Data inconsistency.", orderId);
            // Consider error handling: dead-letter queue, retry mechanism, or alert.
            // This could happen if the Order creation event was missed, or DB was purged.
        }
    }


    /**
     * Handles an incoming trade event from Kafka.
     * This method is typically called by a Kafka consumer configured to listen to the
     * topic where MatchingEngine publishes Trade objects.
     * It saves the new trade record to the database.
     *
     * @param trade The Trade object received from Kafka.
     */
    @Transactional
    public void handleTradeEvent(Trade trade) {
        if (trade == null || trade.getTradeId() == null) {
            log.warn("Received null or invalid trade event. Skipping processing.");
            return;
        }

        log.info("Processing new trade event: Trade ID={}, Price={}, Quantity={}",
                trade.getTradeId(), trade.getTradedPrice(), trade.getTradedQuantity());

        // Check if trade already exists to prevent duplicates (idempotency)
        // Kafka consumers might reprocess messages.
        Optional<Trade> existingTradeOptional = tradeRepository.findByTradeId(trade.getTradeId());
        if (existingTradeOptional.isPresent()) {
            log.warn("Trade ID: {} already exists in database. Skipping duplicate trade event.", trade.getTradeId());
            return;
        }

        try {
            tradeRepository.save(trade);
            log.info("Trade ID: {} successfully saved to DB.", trade.getTradeId());
        } catch (Exception e) {
            log.error("Failed to save trade {}: {}", trade.getTradeId(), e.getMessage(), e);
            // Handle unique constraint violation or other DB errors
        }
    }

    /**
     * NEW: Handles an OrderConfirmedCancellationEvent from Kafka.
     * This method updates the order's status to "CANCELED" in the database.
     * This event signifies that the MatchingEngine successfully processed a cancellation request.
     *
     * @param event The OrderConfirmedCancellationEvent object received from Kafka.
     */
    @Transactional
    public void handleOrderConfirmedCancelledEvent(OrderConfirmedCancellationEvent event) {
        if (event == null || event.getOrderId() == null) {
            log.warn("Received null or invalid OrderConfirmedCancellationEvent. Skipping processing.");
            return;
        }

        log.info("Processing OrderConfirmedCancellationEvent for Order ID: {}", event.getOrderId());
        Optional<Order> existingOrderOptional = orderRepository.findByOrderId(event.getOrderId());

        if (existingOrderOptional.isPresent()) {
            Order existingOrder = existingOrderOptional.get();

            // Only update if the order isn't already FILLED.
            // A match event might have arrived first and already filled the order,
            // in which case a cancellation confirmation is irrelevant for state.
            if (existingOrder.getStatus() == OrderStatus.FILLED) {
                log.warn("Order ID: {} is already FILLED. Ignoring OrderConfirmedCancellationEvent.", existingOrder.getOrderId());
                // This means the order filled and then a cancellation was confirmed by ME.
                // This implies a very fast fill or a delayed cancel.
                // The filled status takes precedence.
                return;
            }

            // If the order is already CANCELED, it's likely a duplicate event or already handled.
            if (existingOrder.getStatus() == OrderStatus.CANCELED) {
                log.warn("Order ID: {} is already CANCELED in DB. Ignoring duplicate OrderConfirmedCancellationEvent.", existingOrder.getOrderId());
                return;
            }

            // If the order is OPEN or PARTIALLY_FILLED, proceed with cancellation.
            if (existingOrder.getStatus() == OrderStatus.OPEN || existingOrder.getStatus() == OrderStatus.PARTIALLY_FILLED) {
                existingOrder.setStatus(OrderStatus.CANCELED);
                existingOrder.setRemainingQuantity(BigDecimal.ZERO); // No remaining quantity to cancel
                existingOrder.setTimestamp(Instant.now());

                try {
                    orderRepository.save(existingOrder);
                    log.info("Order ID: {} successfully updated to CANCELED in DB.", existingOrder.getOrderId());
                } catch (Exception e) {
                    log.error("Failed to save confirmed cancelled order {} due to DB error or optimistic locking conflict: {}", event.getOrderId(), e.getMessage(), e);
                }
            } else {
                log.warn("Order ID: {} is in an unexpected state ({}) for OrderConfirmedCancellationEvent. Skipping update.", existingOrder.getOrderId(), existingOrder.getStatus());
            }
        } else {
            log.error("Order ID: {} not found in database for OrderConfirmedCancellationEvent. Data inconsistency.", event.getOrderId());
            // This might warrant alerting or further investigation.
        }
    }

    /**
     * NEW: Handles an OrderCancellationRejectedEvent from Kafka.
     * This method logs the rejection and ensures the order's status reflects its actual (non-canceled) state.
     * This event signifies that the MatchingEngine rejected a cancellation request (e.g., order was already filled).
     *
     * @param event The OrderCancellationRejectedEvent object received from Kafka.
     */
    @Transactional
    public void handleOrderCancellationRejectedEvent(OrderCancellationRejectedEvent event) {
        if (event == null || event.getOrderId() == null) {
            log.warn("Received null or invalid OrderCancellationRejectedEvent. Skipping processing.");
            return;
        }

        log.warn("Order cancellation for Order ID: {} was REJECTED by MatchingEngine. Reason: {}",
                event.getOrderId(), event.getReason());

        Optional<Order> existingOrderOptional = orderRepository.findByOrderId(event.getOrderId());
        if (existingOrderOptional.isPresent()) {
            Order existingOrder = existingOrderOptional.get();

            if (existingOrder.getStatus() == OrderStatus.CANCELED) {
                log.error("CRITICAL INCONSISTENCY: Order {} is CANCELED in DB but cancellation was REJECTED by ME (Reason: {}). " +
                                "Attempting to revert status to OPEN/PARTIALLY_FILLED/FILLED based on remaining/filled quantity.",
                        existingOrder.getOrderId(), event.getReason());

                if (existingOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) <= 0) {
                    existingOrder.setStatus(OrderStatus.FILLED);
                    log.info("Order ID: {} status reverted from CANCELED to FILLED based on rejection reason.", existingOrder.getOrderId());
                } else if (existingOrder.getFilledQuantity().compareTo(BigDecimal.ZERO) > 0) {
                    existingOrder.setStatus(OrderStatus.PARTIALLY_FILLED);
                    log.info("Order ID: {} status reverted from CANCELED to PARTIALLY_FILLED based on rejection reason.", existingOrder.getOrderId());
                } else {
                    existingOrder.setStatus(OrderStatus.OPEN);
                    log.info("Order ID: {} status reverted from CANCELED to OPEN based on rejection reason.", existingOrder.getOrderId());
                }
                existingOrder.setTimestamp(Instant.now()); // Update timestamp

                try {
                    orderRepository.save(existingOrder);
                    log.info("Order ID: {} status successfully adjusted in DB after cancellation rejection.", existingOrder.getOrderId());
                } catch (Exception e) {
                    log.error("Failed to save adjusted order {} after cancellation rejection due to DB error: {}", event.getOrderId(), e.getMessage(), e);
                }
            } else {
                log.info("Order ID: {} status (current in DB: {}) is consistent after cancellation rejection (Reason: {}). No change needed.",
                        existingOrder.getOrderId(), existingOrder.getStatus(), event.getReason());
            }
        } else {
            log.error("Order ID: {} not found in database for cancellation rejection event. Data inconsistency.", event.getOrderId());
            // This might warrant alerting or further investigation.
        }
    }
}