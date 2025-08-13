package treiding.hpq.userservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.kafka.BaseKafkaProducer;
import treiding.hpq.basedomain.kafkaevent.userevent.UserCreatedEvent;

@Component
public class UserCreatedProducer extends BaseKafkaProducer<UserCreatedEvent> {

    public static final String TOPIC_NAME = "user.create";

    public UserCreatedProducer(KafkaTemplate<String, UserCreatedEvent> kafkaTemplate) {
        super(kafkaTemplate);
    }

    /**
     * Publishes a UserCreatedEvent to the designated topic.
     * @param event The event to publish.
     */
    public void publishUserCreatedEvent(UserCreatedEvent event) {
        // We'll use the userId as the message key.
        // This ensures all events for the same user go to the same partition.
        sendMessage(TOPIC_NAME, event.getUserId(), event);
    }
}
