package treiding.hpq.basedomain.kafkaevent.userevent;

import java.io.Serializable;
import java.time.LocalDateTime;

public class UserCreatedEvent implements Serializable {
    private String userId;
    private LocalDateTime timestamp;

    public UserCreatedEvent() {
    }

    public UserCreatedEvent(String userId) {
        this.userId = userId;
        this.timestamp = LocalDateTime.now();
    }

    // Getters and Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserCreatedEvent{" +
                "userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}