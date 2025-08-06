package treiding.hpq.walletservice.service;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;
import treiding.hpq.walletservice.dto.WalletBalanceResponse;
import treiding.hpq.walletservice.entity.Wallet;
import treiding.hpq.walletservice.repository.WalletRepository;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class WalletManagerService {

    private final WalletRepository walletRepository;

    public WalletManagerService(WalletRepository walletRepository) {
        this.walletRepository = walletRepository;
    }

    public List<WalletBalanceResponse> getBalancesByUserId(String userId) {
        return walletRepository.findByUserId(userId).stream()
                .map(wallet -> new WalletBalanceResponse(wallet.getCurrency(), wallet.getBalance(), wallet.getAvailableBalance()))
                .collect(Collectors.toList());
    }

    @Transactional
    public void createInitialWallets(String userId) {
        if (walletRepository.findByUserIdAndCurrency(userId, "USDT").isEmpty()) {
            Wallet usdtWallet = new Wallet();
            usdtWallet.setUserId(userId);
            usdtWallet.setCurrency("USDT");
            usdtWallet.setBalance(new BigDecimal("10000.00")); // Initial balance for demo
            usdtWallet.setAvailableBalance(new BigDecimal("10000.00"));
            walletRepository.save(usdtWallet);
        }
        if (walletRepository.findByUserIdAndCurrency(userId, "BTC").isEmpty()) {
            Wallet btcWallet = new Wallet();
            btcWallet.setUserId(userId);
            btcWallet.setCurrency("BTC");
            btcWallet.setBalance(BigDecimal.ZERO);
            btcWallet.setAvailableBalance(BigDecimal.ZERO);
            walletRepository.save(btcWallet);
        }
    }
}