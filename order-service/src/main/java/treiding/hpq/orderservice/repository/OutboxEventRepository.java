package treiding.hpq.orderservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import treiding.hpq.orderservice.outbox.OutboxEvent;
import treiding.hpq.orderservice.outbox.OutboxStatus;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    // Tìm các sự kiện đang chờ xử lý (PENDING), giới hạn số lượng để tránh tải quá nhiều
    List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus status);
}