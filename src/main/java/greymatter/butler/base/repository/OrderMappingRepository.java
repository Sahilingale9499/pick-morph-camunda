package greymatter.butler.base.repository;

import greymatter.butler.base.model.OrderMapping;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderMappingRepository extends JpaRepository<OrderMapping, Long> {
    List<OrderMapping> findByParentExternalServiceRequestId(String parentExternalServiceRequestId);
}
