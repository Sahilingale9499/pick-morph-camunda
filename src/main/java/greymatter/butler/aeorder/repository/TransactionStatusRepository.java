package greymatter.butler.aeorder.repository;

import greymatter.butler.aeorder.model.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionStatusRepository extends JpaRepository<TransactionStatus, String> {
}
