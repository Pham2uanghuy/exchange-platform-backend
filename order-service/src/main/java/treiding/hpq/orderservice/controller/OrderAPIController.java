package treiding.hpq.orderservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.exception.OrderCancellationException;
import treiding.hpq.basedomain.exception.OrderNotFoundException;
import treiding.hpq.orderservice.exception.InsufficientFundsException;
import treiding.hpq.orderservice.service.api.OrderService;

@RestController
@RequestMapping("/api/orders")
public class OrderAPIController {

    private final OrderService orderService;

    public OrderAPIController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Endpoint to submit a new order (BUY or SELL).
     * Request Body: JSON representation of an Order (price, quantity, side, instrumentId, userId)
     * Response: The updated Order entity with its ID and current status.
     */
    @PostMapping
    public ResponseEntity<Order> placeOrder(@RequestBody Order order) {
//        Order savedOrder = orderService.createOrder(order);
//        return new ResponseEntity<>(savedOrder, HttpStatus.CREATED);
        try {
            Order savedOrder = orderService.createOrder(order);
            return new ResponseEntity<>(savedOrder, HttpStatus.CREATED);
        } catch (InsufficientFundsException e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Endpoint to cancel an existing order by its ID.
     * @param orderId The unique identifier of the order to cancel.
     * @return A ResponseEntity indicating the success or failure of the cancellation.
     */
    @DeleteMapping("/{orderId}")
    public ResponseEntity<Void> cancelOrder(@PathVariable String orderId) {
        orderService.cancelOrder(orderId);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
