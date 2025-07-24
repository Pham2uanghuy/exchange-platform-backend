package treiding.hpq.orderstatusservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.orderstatusservice.repository.OrderRepository;
import treiding.hpq.orderstatusservice.repository.TradeRepository;
import org.springframework.transaction.annotation.Transactional; // For transactional database operations

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
     * Handles an incoming order status update event from Kafka.
     * This method is typically called by a Kafka consumer.
     * It updates the order's status and quantities in the database.
     *
     * @param updatedOrder The Order object received from Kafka with updated status and quantities.
     */
    @Transactional // Ensures atomicity for database operations
    public void handleOrderStatusUpdate(Order updatedOrder) {
        if (updatedOrder == null || updatedOrder.getOrderId() == null) {
            log.warn("Received null or invalid order status update event. Skipping processing.");
            return;
        }

        log.info("Processing order status update for Order ID: {} to status: {}. Remaining Qty: {}",
                updatedOrder.getOrderId(), updatedOrder.getStatus(), updatedOrder.getRemainingQuantity());

        Optional<Order> existingOrderOptional = orderRepository.findByOrderId(updatedOrder.getOrderId());

        if (existingOrderOptional.isPresent()) {
            Order existingOrder = existingOrderOptional.get();

            // Only update if the incoming update reflects a later or more definitive state.
            // For example, if a NEW order comes in, but we already have it as FILLED, don't revert.
            // This assumes updates are generally progressive (OPEN -> PARTIAL -> FILLED/CANCELED).
            // You might need more sophisticated versioning or timestamp checks for complex scenarios.

            // A simple check: if the status is already FILLED, don't process further updates
            if (existingOrder.getStatus() == OrderStatus.FILLED && updatedOrder.getStatus() != OrderStatus.FILLED) {
                log.warn("Received update for already FILLED order {}. Current status: {}. Incoming status: {}. Skipping.",
                        existingOrder.getOrderId(), existingOrder.getStatus(), updatedOrder.getStatus());
                return;
            }

            // Update relevant fields
            existingOrder.setStatus(updatedOrder.getStatus());
            existingOrder.setRemainingQuantity(updatedOrder.getRemainingQuantity());
            existingOrder.setFilledQuantity(updatedOrder.getOriginalQuantity().subtract(updatedOrder.getRemainingQuantity()));
            existingOrder.setTimestamp(updatedOrder.getTimestamp()); // Use the timestamp from the update (Matching Engine's time)

            orderRepository.save(existingOrder);
            log.info("Order ID: {} successfully updated to status: {} in DB. Remaining Qty: {}",
                    existingOrder.getOrderId(), existingOrder.getStatus(), existingOrder.getRemainingQuantity());

            // If the order is now fully filled or canceled, consider removing it from any active caches
            // or perform other cleanup if necessary.
            if (existingOrder.getStatus() == OrderStatus.FILLED || existingOrder.getStatus() == OrderStatus.CANCELED) {
                log.info("Order ID: {} is now final (FILLED/CANCELED).", existingOrder.getOrderId());
            }

        } else {
            // This case should ideally not happen for updates originating from Matching Engine,
            // as orders are created by Order Service first.
            log.warn("Order ID: {} not found in database for status update. This indicates a potential data inconsistency.", updatedOrder.getOrderId());
            // Depending on your system, you might:
            // - Log an error and alert.
            // - Re-request the initial order.
            // - Save it if it's considered a "new" (though unexpected) order.
        }
    }

    /**
     * Handles an incoming trade event from Kafka.
     * This method is typically called by a Kafka consumer.
     * It saves the new trade record to the database.
     *
     * @param trade The Trade object received from Kafka.
     */
    @Transactional // Ensures atomicity for database operations
    public void handleTradeEvent(Trade trade) {
        if (trade == null || trade.getTradeId() == null) {
            log.warn("Received null or invalid trade event. Skipping processing.");
            return;
        }

        log.info("Processing new trade event: Trade ID={}, Price={}, Quantity={}",
                trade.getTradeId(), trade.getTradedPrice(), trade.getTradedQuantity());

        // Check if trade already exists to prevent duplicates (idempotency)
        // This is crucial if Kafka consumers might reprocess messages.
        Optional<Trade> existingTradeOptional = tradeRepository.findByTradeId(trade.getTradeId());
        if (existingTradeOptional.isPresent()) {
            log.warn("Trade ID: {} already exists in database. Skipping duplicate trade event.", trade.getTradeId());
            return;
        }

        tradeRepository.save(trade);
        log.info("Trade ID: {} successfully saved to DB.", trade.getTradeId());
    }

    /**
     * Handles new incoming orders published by Order Service to Kafka.
     * This might be used if the Matching Engine is the primary "consumer" of new orders,
     * but other services also need to consume them for, e.g., auditing or other processes.
     * For the Matching Engine's perspective, this isn't strictly needed if it relies
     * on initial load and OrderCommandProducer from OrderServiceIpml.
     *
     * In a typical setup, OrderServiceIpml creates the order and publishes it.
     * MatchingEngine consumes it as a 'new' order.
     * OrderStatusUpdateService consumes updates *from* MatchingEngine.
     * So, this method might be less critical if OrderServiceIpml handles initial creation.
     * However, if you have a distributed setup where different services create orders,
     * this could be a generic handler for any new order event.
     *
     * @param newOrder The new Order object received from Kafka.
     */
    @Transactional
    public void handleNewOrderEvent(Order newOrder) {
        // This service's primary role is UPDATES and TRADES generated by the Matching Engine.
        // New order creation is usually handled by the OrderServiceIpml directly saving to DB.
        // If this method is called, it implies a new order created elsewhere needs to be persisted here.
        log.warn("Received a 'new order' event via Kafka. This service primarily handles status updates and trades. " +
                "Order ID: {}. Ensuring it exists in DB.", newOrder.getOrderId());

        Optional<Order> existingOrderOptional = orderRepository.findByOrderId(newOrder.getOrderId());
        if (existingOrderOptional.isEmpty()) {
            // Save the new order if it doesn't already exist.
            // This is a defensive measure; ideally, OrderServiceIpml handles initial save.
            orderRepository.save(newOrder);
            log.info("New Order ID: {} successfully saved to DB (from Kafka event).", newOrder.getOrderId());
        } else {
            log.debug("New Order ID: {} already exists in DB. Skipping save (from Kafka event).", newOrder.getOrderId());
        }
    }
}