package treiding.hpq.orderstatusservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderCancellationRejectedEvent;
import treiding.hpq.basedomain.kafkaevent.orderevent.OrderConfirmedCancellationEvent;

import java.io.IOException;

public class OrderCancellationEventDeserializer implements org.apache.kafka.common.serialization.Deserializer<Object> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderCancellationEventDeserializer() {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Override
    public void configure(java.util.Map<java.lang.String, ?> configs, boolean isKey) {
        // No specific configuration needed here
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            // Try to deserialize as OrderConfirmedCancellationEvent first
            return objectMapper.readValue(data, OrderConfirmedCancellationEvent.class);
        } catch (IOException e1) {
            try {
                // If that fails, try OrderCancellationRejectedEvent
                return objectMapper.readValue(data, OrderCancellationRejectedEvent.class);
            } catch (IOException e2) {
                // Log and throw if neither works
                throw new RuntimeException("Failed to deserialize OrderCancellation event: " + e2.getMessage(), e2);
            }
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}

