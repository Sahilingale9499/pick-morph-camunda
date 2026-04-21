package greymatter.butler.aeorder.repository;

import greymatter.butler.aeorder.model.AeOrdersMapping;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AeOrdersMappingRepository extends JpaRepository<AeOrdersMapping, Long> {
    List<AeOrdersMapping> findByParentExternalServiceRequestId(String parentExternalServiceRequestId);
}
