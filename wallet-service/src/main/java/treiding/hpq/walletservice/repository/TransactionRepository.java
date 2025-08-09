package treiding.hpq.walletservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import treiding.hpq.walletservice.entity.Transaction;
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {
}
