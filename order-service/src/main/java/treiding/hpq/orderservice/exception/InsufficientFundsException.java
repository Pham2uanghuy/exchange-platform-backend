package treiding.hpq.orderservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InsufficientFundsException extends RuntimeException {

    /**
     * Constructs a new InsufficientFundsException with the specified detail message.
     * @param message The detail message (e.g., "Insufficient funds for USDT.").
     */
    public InsufficientFundsException(String message) {
        super(message);
    }
}
