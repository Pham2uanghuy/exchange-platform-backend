package treiding.hpq.walletservice.kafka;



import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import treiding.hpq.basedomain.kafka.BaseKafkaConsumer;
import treiding.hpq.basedomain.kafkaevent.userevent.UserCreatedEvent;
import treiding.hpq.walletservice.service.WalletManagerService;

import java.util.Properties;

@Component
public class UserCreatedConsumer extends BaseKafkaConsumer<UserCreatedEvent> {

    public static final String USER_CREATED_TOPIC = "user.create";
    private final WalletManagerService walletManagerService;


    public UserCreatedConsumer(
            @Value("${spring.kafka.consumer.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id.user-create}") String groupId,
            WalletManagerService walletManagerService) {
        super(buildConsumerProps(bootstrapServers, groupId), USER_CREATED_TOPIC, true, 1);
        this.walletManagerService = walletManagerService;
    }

    /**
     * Helper method to build Kafka consumer properties for trade events.
     * @param bootstrapServers Kafka bootstrap servers.
     * @param groupId Consumer group ID.
     * @return Properties object for Kafka consumer.
     */
    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, UserCreatedEvent.class.getName());

        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        return props;
    }

    @Override
    protected Consumer<String, UserCreatedEvent> createKafkaConsumer(Properties consumerProps, String topicName) {
        return new KafkaConsumer<>(consumerProps);
    }

    @PostConstruct
    public void startListening() {
        log.info("Starting UserCreatedConsumer for topic: {}", USER_CREATED_TOPIC);
        startConsuming(USER_CREATED_TOPIC, this::processUserCreatedEvent);
    }

    @PreDestroy
    public void stopListening() {
        stopConsuming();
    }

    /**
     * This method is the event handler for UserCreatedEvents.
     * @param event The UserCreatedEvent received from Kafka.
     */
    private void processUserCreatedEvent(UserCreatedEvent event) {
        log.info("Processing new user event for userId: {}", event.getUserId());
        try {
            walletManagerService.createInitialWallets(event.getUserId());
            log.info("Successfully created initial wallets for userId: {}", event.getUserId());
        } catch (Exception e) {
            log.error("Failed to create wallets for userId: {}. Error: {}", event.getUserId(), e.getMessage());
        }
    }
}
