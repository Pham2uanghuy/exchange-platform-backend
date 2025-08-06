package treiding.hpq.walletservice.controller;

import org.apache.coyote.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import treiding.hpq.walletservice.dto.WalletBalanceResponse;
import treiding.hpq.walletservice.entity.Wallet;
import treiding.hpq.walletservice.service.WalletManagerService;

import java.util.List;

@RestController
@RequestMapping("/api/wallets")
public class WalletController {

    private final WalletManagerService walletManagerService;

    public WalletController(WalletManagerService walletManagerService) {
        this.walletManagerService = walletManagerService;
    }

    /**
     * Retrieves the wallet balances for the authenticated user.
     * Requires a valid JWT token.
     * @param userId
     * @return A list of wallet balances.
     */
    @GetMapping("/balances")
    public ResponseEntity<List<WalletBalanceResponse>> getWalletBalances(String userId) {
        List<WalletBalanceResponse> balances = walletManagerService.getBalancesByUserId(userId);
        return ResponseEntity.ok(balances);
    }

    @PostMapping("/initWallet/{userId}")
    public ResponseEntity<List<WalletBalanceResponse>> initialWallet(@PathVariable String userId) {
        walletManagerService.createInitialWallets(userId);
        List<WalletBalanceResponse> balances = walletManagerService.getBalancesByUserId(userId);
        return ResponseEntity.ok(balances);
    }
}
