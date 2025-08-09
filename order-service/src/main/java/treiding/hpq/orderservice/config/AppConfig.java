package treiding.hpq.orderservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class AppConfig {

    /**
     * Creates and provides a RestTemplate bean.
     * RestTemplate is a synchronous client for making HTTP requests.
     * It's used here to communicate with other services, like the Wallet Service.
     * @return A new instance of RestTemplate.
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}