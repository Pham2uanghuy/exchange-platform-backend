package treiding.hpq.marketdataservice.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * Configures the message broker.
     * @param config The MessageBrokerRegistry to configure.
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Enables a simple in-memory broker. Messages whose destination starts with "/topic"
        // will be routed to the message broker for broadcasting to subscribed clients.
        config.enableSimpleBroker("/topic");

        // Sets the prefix for application-defined messages. Messages whose destination
        // starts with "/app" will be routed to @MessageMapping methods in @Controller classes.
        config.setApplicationDestinationPrefixes("/app");
    }

    /**
     * Registers STOMP endpoints that clients will use to connect to the WebSocket server.
     * @param registry The StompEndpointRegistry to register endpoints with.
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Registers the "/ws" endpoint. Clients will connect to ws://localhost:8082/ws
        // .setAllowedOriginPatterns("*") allows connections from any origin (for development).
        // .withSockJS() provides fallback options for browsers that don't support WebSockets.
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS();
    }
}