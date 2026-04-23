package greymatter.butler.aeorder.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "transaction_status")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionStatus {

    @Id
    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    @Column(name = "pick_instruction_id")
    private String pickInstructionId;

    @Column(name = "status")
    private String status; // IN_PROGRESS, SUCCESS, FAILED

    @Column(name = "last_updated")
    private Instant lastUpdated;

    /** Raw transaction data from the pick_transaction event (containerAttributes, products, etc.). */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private String payload;
}
