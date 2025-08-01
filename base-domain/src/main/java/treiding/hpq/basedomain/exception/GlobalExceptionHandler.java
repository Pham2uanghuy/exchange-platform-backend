package treiding.hpq.basedomain.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<String> handleOrderNotFoundException(OrderNotFoundException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(OrderCancellationException.class)
    public ResponseEntity<String> handleOrderCancellationException(OrderCancellationException ex) {
        if (ex.getMessage().contains("fully matched")) {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.CONFLICT); // HTTP 409
        } else {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST); // HTTP 400
        }
    }
}
