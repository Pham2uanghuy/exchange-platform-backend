package treiding.hpq.walletservice.entity;



public enum TransactionType {
    /**
     * Represents a credit (inflow) of funds resulting from a trade.
     * For example, the seller receiving USDT or the buyer receiving BTC.
     */
    TRADE_CREDIT,

    /**
     * Represents a debit (outflow) of funds resulting from a trade.
     * For example, the seller sending BTC or the buyer sending USDT.
     */
    TRADE_DEBIT,

    // --- Other potential transaction types you might need in the future ---

    /**
     * Represents a deposit of funds into a user's wallet.
     */
    DEPOSIT,

    /**
     * Represents a withdrawal of funds from a user's wallet.
     */
    WITHDRAWAL,

    /**
     * Represents a fee charged for a specific service (e.g., trading fee, withdrawal fee).
     */
    FEE,

    /**
     * Represents a manual adjustment to a user's wallet balance by an administrator.
     */
    ADJUSTMENT
}