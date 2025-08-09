package treiding.hpq.walletservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import treiding.hpq.walletservice.entity.Wallet;

import java.util.List;
import java.util.Optional;

@Repository
public interface WalletRepository extends JpaRepository<Wallet, Long> {
    Optional<Wallet> findByUserIdAndCurrency(String userId, String currency);
    List<Wallet> findByUserId(String userId);
}
