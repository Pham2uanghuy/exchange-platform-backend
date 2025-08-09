package treiding.hpq.walletservice.dto;

import java.math.BigDecimal;

public class WalletBalanceResponse {

    /**
     * The currency code of the wallet (e.g., "BTC", "USDT").
     */
    private String currency;

    /**
     * The total balance in the wallet, including any funds currently reserved for open orders.
     */
    private BigDecimal balance;

    /**
     * The available balance that can be used for placing new orders.
     * This is the total balance minus any reserved funds.
     */
    private BigDecimal availableBalance;

    public WalletBalanceResponse() {
    }

    public BigDecimal getAvailableBalance() {
        return availableBalance;
    }

    public void setAvailableBalance(BigDecimal availableBalance) {
        this.availableBalance = availableBalance;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public WalletBalanceResponse(String currency, BigDecimal balance, BigDecimal availableBalance) {
        this.currency = currency;
        this.balance = balance;
        this.availableBalance = availableBalance;
    }
}