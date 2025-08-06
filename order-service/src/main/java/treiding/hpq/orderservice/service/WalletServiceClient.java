package treiding.hpq.orderservice.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Optional;

@Service
public class WalletServiceClient {

    private final RestTemplate restTemplate;
    private final String walletServiceUrl;

    public WalletServiceClient(
            RestTemplate restTemplate,
            @Value("${wallet-service.url}") String walletServiceUrl) {
        this.restTemplate = restTemplate;
        this.walletServiceUrl = walletServiceUrl;
    }

    /**
     * Calls the Wallet Service to get the available balance without JWT.
     * @param userId The ID of the user.
     * @param currency The currency to check (e.g., "USDT", "BTC").
     * @return An Optional containing the available balance, or empty if an error occurs.
     */
    public Optional<BigDecimal> getAvailableBalance(String userId, String currency) {
        try {
            String url = String.format("%s/api/wallets/check-balance?userId=%s&currency=%s", walletServiceUrl, userId, currency);
            ResponseEntity<BigDecimal> response = restTemplate.getForEntity(url, BigDecimal.class);
            return Optional.ofNullable(response.getBody());
        } catch (Exception e) {
            System.err.println("Error calling Wallet Service to get available balance: " + e.getMessage());
            return Optional.empty();
        }
    }
}
