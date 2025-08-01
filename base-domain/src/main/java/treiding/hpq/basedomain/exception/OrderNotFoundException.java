package treiding.hpq.basedomain.exception;

public class OrderNotFoundException extends RuntimeException{
    public OrderNotFoundException(String msg) {
        super(msg);
    }
}
