package treiding.hpq.basedomain.exception;

public class OrderCancellationException extends RuntimeException{
    public OrderCancellationException(String msg) {
        super(msg);
    }
}
