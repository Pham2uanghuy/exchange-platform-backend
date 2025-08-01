package treiding.hpq.orderservice.service.impl;

import jakarta.annotation.PostConstruct;
import jakarta.persistence.OptimisticLockException;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.basedomain.exception.OrderCancellationException;
import treiding.hpq.basedomain.exception.OrderNotFoundException;
import treiding.hpq.orderservice.kafka.OrderCommandProducer;
import treiding.hpq.orderservice.kafka.OrderInitialLoadProducer;
import treiding.hpq.orderservice.outbox.OutboxEvent;
import treiding.hpq.orderservice.repository.OrderRepository;
import treiding.hpq.orderservice.repository.OutboxEventRepository;
import treiding.hpq.orderservice.service.api.OrderService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


@Service
public class OrderServiceIpml implements OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderServiceIpml.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderInitialLoadProducer orderInitialLoadProducer;

    // Constructor now takes the new Kafka Producers
    public OrderServiceIpml(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository,
                            OrderInitialLoadProducer orderInitialLoadProducer) {       // Inject new producer
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.orderInitialLoadProducer = orderInitialLoadProducer;
    }

    @PostConstruct
    public void init() {
        publishAllOpenOrdersToKafka();
    }

    /**
     * Handles a new incoming order from a user.
     * The order is saved to DB and then published as a real-time event to Kafka.
     * @param order The order object received from the API.
     * @return The initial saved order object. Its final state will be updated via listeners.
     */
    @Override
    @Transactional
    public Order createOrder (Order order) {
        // Generate UUID for the order and set initial status
        order.setOrderId(UUID.randomUUID().toString());
        order.setTimestamp(Instant.now());
        order.setRemainingQuantity(order.getOriginalQuantity());
        order.setFilledQuantity(BigDecimal.valueOf(0.0));
        order.setStatus(OrderStatus.OPEN);

        // save to db
        Order savedOrder = orderRepository.save(order);
        log.info("Saved new order to DB (initial state): {}", savedOrder.getOrderId());

        // create outbox
        OutboxEvent outboxEvent = OutboxEvent.createOrderCreatedEvent(savedOrder, OrderCommandProducer.ORDER_COMMANDS_TOPIC);
        outboxEventRepository.save(outboxEvent);
        log.info("Recorded new order event to outbox for ID: {}. Outbox event ID: {}", savedOrder.getOrderId(), outboxEvent.getId());

        return savedOrder;
    }

    /**
     * Get orders placed by a user
     * @return List of the orders
     */
    @Override
    public List<Order> getOrders() {
        log.debug("Attempt to retrieve orders");
        List<Order> orders = orderRepository.findAll();
        if (orders.isEmpty()) {
            log.debug("Orders not found in DB.");
        } else {
            log.debug("Orders found in DB.");
        }
        return orders;
    }

    /**
     * Get a specified order placed by user with orderId
     * @param orderId The ID of the order to retrieve.
     * @return Order matched with orderId param, or null if not found.
     */
    @Override
    public Order getOrderByOrderId(String orderId) {
        log.debug("Attempting to retrieve order with ID: {}.", orderId);
        Optional<Order> orderOptional = orderRepository.findByOrderId(orderId);
        if (orderOptional.isPresent()) {
            log.debug("Order {} found in DB.", orderId);
        } else {
            log.debug("Order {} not found in DB.", orderId);
        }
        return orderOptional.orElse(null);
    }

    /**
     * Cancels an existing order identified by its ID.
     * This method is designed to handle concurrent modifications (e.g., matching events)
     * using Optimistic Locking to ensure data consistency and prioritize matched orders.
     *
     * @param orderId The unique identifier of the order to cancel.
     * @throws OrderNotFoundException if the specified order does not exist.
     * @throws OrderCancellationException if the order cannot be canceled due to its current state
     * (e.g., already matched, already canceled, or other invalid states).
     */
    @Override
    @Transactional
    public void cancelOrder(String orderId) {
        log.info("Attempting to cancel order with ID: {}.", orderId);
        // Retrieve the order from the database
        Order orderToCancel = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> {
                    log.warn("Order {} not found for cancellation.", orderId);
                    // Throw custom exception for API layer to map to HTTP 404 Not Found.
                    return new OrderNotFoundException("Order with ID " + orderId + " not found.");
                });

        OrderStatus currentStatus = orderToCancel.getStatus();

        // Evaluate the current status of the order for cancellation eligibility.
        if (currentStatus == OrderStatus.OPEN || currentStatus == OrderStatus.PARTIALLY_FILLED) {
            // Order is OPEN or PARTIALLY_FILLED. It is eligible for cancellation.
            // Record 'Order Cancelled' event in the Outbox. This event will be sent to Kafka
            // Matching Service  receive the msg then remove the order from its order book.
            OutboxEvent outboxEventForMatching = OutboxEvent.createOrderCancelledEvent(orderToCancel, OrderCommandProducer.ORDER_COMMANDS_TOPIC);
            outboxEventRepository.save(outboxEventForMatching);
            log.info("Recorded order cancellation event to outbox for ID: {}. Outbox event ID: {}", orderId, outboxEventForMatching.getId());

        } else if (currentStatus == OrderStatus.FILLED) {
            // Order is FULLY MATCHED. It cannot be canceled.
            log.warn("Order {} cannot be cancelled: It is already FULLY MATCHED.", orderId);
            // Throw custom exception for API layer to map to HTTP 409 Conflict.
            throw new OrderCancellationException("Order " + orderId + " has been fully matched and cannot be canceled.");

        } else if (currentStatus == OrderStatus.CANCELED) {
            // Order is already CANCELED. No action needed.
            log.warn("Order {} cannot be cancelled: It is already CANCELED.", orderId);
            // Throw custom exception for API layer to map to HTTP 400 Bad Request.
            throw new OrderCancellationException("Order " + orderId + " is already canceled.");

        } else {
            // Other statuses that do not allow cancellation (e.g., EXPIRED, REJECTED).
            log.warn("Order {} cannot be cancelled: Its current status ({}) does not allow cancellation.", orderId, currentStatus);
            // Throw custom exception for API layer to map to HTTP 400 Bad Request.
            throw new OrderCancellationException("Order " + orderId + " cannot be canceled in its current state (" + currentStatus + ").");
        }

    }

    /**
     * Deletes an order from the database.
     * This might be used for historical cleanup or error handling.
     * Consider if this should also trigger a Kafka event (e.g., OrderDeleted).
     * @param orderId The ID of the order to delete.
     */
    @Override
    public void deleteOrder(String orderId) {
        log.info("Attempting to delete order with ID: {}.", orderId);
        Optional<Order> orderOptional = orderRepository.findByOrderId(orderId);
        if (orderOptional.isPresent()) {
            orderRepository.delete(orderOptional.get());
            log.info("Order {} deleted from DB.", orderId);
        } else {
            log.warn("Order {} not found for deletion.", orderId);
        }
    }

    /**
     * Loads all currently OPEN or PARTIALLY_FILLED orders from the database
     * and publishes them to a dedicated Kafka topic for initial synchronization.
     * This method is typically called once on application startup or when
     * a new consumer (like a Matching Engine) needs to rebuild its state.
     */
    public void publishAllOpenOrdersToKafka() {
        log.info("Starting to load and publish all OPEN/PARTIALLY_FILLED orders from DB to Kafka for initial sync.");

        // Find all orders that are currently active (OPEN or PARTIALLY_FILLED)
        List<Order> activeOrders = orderRepository.findAll().stream()
                .filter(order -> order.getStatus() == OrderStatus.OPEN || order.getStatus() == OrderStatus.PARTIALLY_FILLED)
                .toList();

        if (activeOrders.isEmpty()) {
            log.info("No active orders found in the database to publish for initial sync.");
            return;
        }
        String topic = orderInitialLoadProducer.getInitialLoadTopic();
        log.info("Found {} active orders in DB. Publishing them to Kafka topic '{}'.",
                activeOrders.size(), topic); // Use the topic name from producer

        for (Order order : activeOrders) {
            orderInitialLoadProducer.sendInitialLoadOrder(order);
            log.debug("Published initial load order for ID: {}", order.getOrderId());
        }

        log.info("Finished publishing all active orders to Kafka for initial synchronization.");
    }
}
