package treiding.hpq.orderservice.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderStatus;
import treiding.hpq.orderservice.kafka.OrderCommandProducer;
import treiding.hpq.orderservice.kafka.OrderInitialLoadProducer;
import treiding.hpq.orderservice.repository.OrderRepository;
import treiding.hpq.orderservice.service.api.OrderService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;


@Service
public class OrderServiceIpml implements OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderServiceIpml.class);

    private final OrderRepository orderRepository;
    private final OrderInitialLoadProducer orderInitialLoadProducer; // NEW FIELD: Producer for initial load

    // You'll also need a producer for real-time order events (new orders, cancellations, etc.)
    // Let's call it OrderCommandProducer, as you mentioned you have one for "order commands."
    private final OrderCommandProducer orderCommandProducer; // NEW FIELD: Producer for real-time commands/events

    // Constructor now takes the new Kafka Producers
    public OrderServiceIpml(OrderRepository orderRepository,
                            OrderInitialLoadProducer orderInitialLoadProducer, // Inject new producer
                            OrderCommandProducer orderCommandProducer) {       // Inject new producer
        this.orderRepository = orderRepository;
        this.orderInitialLoadProducer = orderInitialLoadProducer;
        this.orderCommandProducer = orderCommandProducer;

        publishAllOpenOrdersToKafka();
    }

    /**
     * Handles a new incoming order from a user.
     * The order is saved to DB and then published as a real-time event to Kafka.
     * @param order The order object received from the API.
     * @return The initial saved order object. Its final state will be updated via listeners.
     */
    @Override
    public Order createOrder (Order order) {
        // Generate UUID for the order and set initial status
        order.setOrderId(UUID.randomUUID().toString());
        order.setTimestamp(Instant.now());
        order.setRemainingQuantity(order.getOriginalQuantity());
        order.setFilledQuantity(BigDecimal.valueOf(0.0));
        order.setStatus(OrderStatus.OPEN);

        Order savedOrder = orderRepository.save(order);
        log.info("Saved new order to DB (initial state): {}", savedOrder.getOrderId());

        // Publish the new order to the real-time order events topic
        // This is the "order command" or "order event" that MatchingEngine listens to for new orders.
        orderCommandProducer.sendOrderCommand(savedOrder);
        log.info("Published new order event to Kafka for order ID: {}", savedOrder.getOrderId());

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
     * Cancels an existing order by its ID.
     * Updates the order status in the DB and publishes a cancellation event to Kafka.
     * @param orderId The ID of the order to cancel.
     */
    @Override
    public void cancelOrder(String orderId) {
        log.info("Attempting to cancel order with ID: {}.", orderId);
        Optional<Order> orderOptional = orderRepository.findByOrderId(orderId);

        if (orderOptional.isPresent()) {
            Order orderToCancel = orderOptional.get();
            if (orderToCancel.getStatus() == OrderStatus.OPEN || orderToCancel.getStatus() == OrderStatus.PARTIALLY_FILLED) {
                orderToCancel.setStatus(OrderStatus.CANCELED);
                orderToCancel.setTimestamp(Instant.now()); // Update timestamp to reflect cancellation time
                orderRepository.save(orderToCancel);
                log.info("Order {} status updated to CANCELED in DB.", orderId);

                // Publish the cancellation event to Kafka
                // Assuming orderCommandProducer can also handle cancellation events
                orderCommandProducer.sendOrderCommand(orderToCancel);
                log.info("Published order cancellation event to Kafka for order ID: {}.", orderId);
            } else {
                log.warn("Order {} cannot be cancelled as its current status is {}.", orderId, orderToCancel.getStatus());
            }
        } else {
            log.warn("Order {} not found for cancellation.", orderId);
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
        // Before deleting, you might want to check if it's safe to delete (e.g., already filled/canceled)
        Optional<Order> orderOptional = orderRepository.findByOrderId(orderId);
        if (orderOptional.isPresent()) {
            orderRepository.delete(orderOptional.get());
            log.info("Order {} deleted from DB.", orderId);
            // Optional: Publish a "OrderDeleted" event if other services need to know
            // orderCommandProducer.sendOrderDeletedEvent(orderId);
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
