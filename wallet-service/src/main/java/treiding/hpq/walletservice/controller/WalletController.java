package treiding.hpq.walletservice.controller;

import org.apache.coyote.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import treiding.hpq.walletservice.dto.WalletBalanceResponse;
import treiding.hpq.walletservice.entity.Wallet;
import treiding.hpq.walletservice.service.WalletManagerService;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/wallets")
public class WalletController {

    private final WalletManagerService walletManagerService;

    public WalletController(WalletManagerService walletManagerService) {
        this.walletManagerService = walletManagerService;
    }

    @GetMapping("/balances")
    public ResponseEntity<List<WalletBalanceResponse>> getWalletBalances(@RequestParam String userId) {
        List<WalletBalanceResponse> balances = walletManagerService.getBalancesByUserId(userId);
        return ResponseEntity.ok(balances);
    }

    @PostMapping("/initWallet/{userId}")
    public ResponseEntity<List<WalletBalanceResponse>> initialWallet(@PathVariable String userId) {
        walletManagerService.createInitialWallets(userId);
        List<WalletBalanceResponse> balances = walletManagerService.getBalancesByUserId(userId);
        return ResponseEntity.ok(balances);
    }

    @GetMapping("/check-balance")
    public ResponseEntity<BigDecimal> getAvailableBalance(
            @RequestParam String userId,
            @RequestParam String currency) {

        BigDecimal availableBalance = walletManagerService.getAvailableBalance(userId, currency);
        return availableBalance != null
                ? ResponseEntity.ok(availableBalance)
                : ResponseEntity.notFound().build();
    }
}

