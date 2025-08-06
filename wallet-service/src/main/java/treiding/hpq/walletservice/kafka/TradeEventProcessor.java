package treiding.hpq.walletservice.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import treiding.hpq.basedomain.entity.Trade;
import treiding.hpq.walletservice.entity.Transaction;
import treiding.hpq.walletservice.entity.TransactionType;
import treiding.hpq.walletservice.entity.Wallet;
import treiding.hpq.walletservice.repository.TransactionRepository;
import treiding.hpq.walletservice.repository.WalletRepository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

@Service
public class TradeEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TradeEventProcessor.class);

    private final WalletRepository walletRepository;
    private final TransactionRepository transactionRepository;

    public TradeEventProcessor(WalletRepository walletRepository, TransactionRepository transactionRepository) {
        this.walletRepository = walletRepository;
        this.transactionRepository = transactionRepository;
    }

    /**
     * Handles a TradeEvent by updating the wallets of the buyer and seller.
     * This method is transactional, ensuring all updates and saves are treated as a single, atomic operation.
     * If any part of the process fails, the entire transaction is rolled back.
     *
     * @param event The TradeEvent received from Kafka.
     */
    @Transactional
    public void handleTradeEvent(Trade event) {
        log.info("Starting to process TradeEvent: {}", event.getTradeId());

        // Extract transaction details from the event
        String buyerId = event.getBuyerId();
        String sellerId = event.getSellerId();
        String instrumentId = event.getInstrumentId();
        String[] currencies = instrumentId.split("/");
        String baseCurrency = currencies[0];
        String quoteCurrency = currencies[1];
        BigDecimal tradePrice = event.getTradedPrice();
        BigDecimal tradeQuantity = event.getTradedQuantity();
        BigDecimal tradeAmount = tradePrice.multiply(tradeQuantity);

        try {
            // Update buyer's wallets
            // Buyer receives base currency (e.g., BTC) and pays with quote currency (e.g., USDT)
            updateWallet(buyerId, baseCurrency, tradeQuantity, TransactionType.TRADE_CREDIT, event.getTradeId(), event.getTimestamp());
            updateWallet(buyerId, quoteCurrency, tradeAmount.negate(), TransactionType.TRADE_DEBIT, event.getTradeId(), event.getTimestamp());

            // Update seller's wallets
            // Seller pays with base currency (e.g., BTC) and receives quote currency (e.g., USDT)
            updateWallet(sellerId, quoteCurrency, tradeAmount, TransactionType.TRADE_CREDIT, event.getTradeId(), event.getTimestamp());
            updateWallet(sellerId, baseCurrency, tradeQuantity.negate(), TransactionType.TRADE_DEBIT, event.getTradeId(), event.getTimestamp());

            log.info("Successfully processed TradeEvent: {}", event.getTradeId());

            // After a successful trade, you might want to also "release" any reserved funds.
            // This logic depends on whether you have a pending order/hold mechanism.
            // We assume for this base version that 'available_balance' and 'balance' are updated together.

        } catch (Exception e) {
            log.error("Failed to process TradeEvent {} due to error. Rolling back transaction.", event.getTradeId(), e);
            // The @Transactional annotation will automatically roll back the changes.
            // If you want to throw a custom exception or handle this error specifically,
            // you can do so here.
            throw new RuntimeException("Trade processing failed.", e);
        }
    }

    /**
     * A helper method to perform the wallet update and transaction logging in one step.
     * This ensures consistency and reduces code duplication.
     *
     * @param userId The ID of the user.
     * @param currency The currency of the wallet to be updated.
     * @param amount The amount to add or subtract from the balance.
     * @param type The type of the transaction (e.g., TRADE_CREDIT, TRADE_DEBIT).
     * @param relatedTradeId The ID of the trade event.
     */
    private void updateWallet(String userId, String currency, BigDecimal amount, TransactionType type, String relatedTradeId, Instant timestamp) {
        Optional<Wallet> walletOpt = walletRepository.findByUserIdAndCurrency(userId, currency);

        if (walletOpt.isPresent()) {
            Wallet wallet = walletOpt.get();
            wallet.setBalance(wallet.getBalance().add(amount));
            wallet.setAvailableBalance(wallet.getAvailableBalance().add(amount));

            walletRepository.save(wallet);

            // Log the transaction for historical purposes
            Transaction transaction = createTransaction(wallet, type, amount, currency, relatedTradeId, timestamp);
            transactionRepository.save(transaction);
        } else {
            // This case indicates a serious data inconsistency (a user involved in a trade
            // without a corresponding wallet). You might want to log this as a critical error.
            log.error("Wallet not found for user {} and currency {} while processing trade {}.", userId, currency, relatedTradeId);
            // For a robust system, you might throw an exception here to trigger a rollback.
            throw new IllegalStateException("Wallet not found for user: " + userId + ", currency: " + currency);
        }
    }

    /**
     * Creates a new Transaction object from the given details.
     * @param wallet The wallet entity involved.
     * @param type The type of transaction.
     * @param amount The amount of the transaction.
     * @param currency The currency of the transaction.
     * @param relatedTradeId The ID of the related trade.
     * @param timestamp The timestamp of the transaction.
     * @return A new Transaction entity.
     */
    private Transaction createTransaction(Wallet wallet, TransactionType type, BigDecimal amount, String currency, String relatedTradeId, Instant timestamp) {
        Transaction transaction = new Transaction();
        transaction.setUserId(wallet.getUserId());
        transaction.setWallet(wallet);
        transaction.setType(type);
        transaction.setAmount(amount);
        transaction.setCurrency(currency);
        transaction.setRelatedTradeId(relatedTradeId);
        transaction.setTimestamp(timestamp);
        return transaction;
    }
}
