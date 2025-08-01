package treiding.hpq.basedomain.kafkaevent;

public enum CancellationReason {
    CANCELED_BY_REQUEST,
    ORDER_NOT_FOUND,
    ORDER_ALREADY_CANCELED,
    ORDER_ALREADY_FILLED,
    INVALID_ORDER_STATE,
    UNKNOWN_REASON // For any other unforeseen circumstances
}
