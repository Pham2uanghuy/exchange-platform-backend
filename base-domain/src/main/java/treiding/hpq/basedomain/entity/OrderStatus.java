package treiding.hpq.basedomain.entity;

/**
 * Represents the current status of an order.
 */
public enum OrderStatus {

    /**
     * The order is active and waiting to be filled.
     */
    OPEN,

    /**
     * The order has been partially filled; some quantity remains.
     */
    PARTIALLY_FILLED,

    /**
     * The order has been completely filled.
     */
    FILLED,

    /**
     * The order was canceled by the user or the system.
     */
    CANCELED
}