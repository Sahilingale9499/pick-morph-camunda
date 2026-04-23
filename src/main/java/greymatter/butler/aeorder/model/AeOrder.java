package greymatter.butler.aeorder.model;

import greymatter.butler.base.model.BaseOrder;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "ae_order")
@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class AeOrder extends BaseOrder {

    @Column(name = "state", nullable = false)
    private String state;

    @Column(name = "sub_state", nullable = false)
    private String subState;

    @Column(name = "is_deleted", nullable = false)
    private Boolean isDeleted;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "stages", columnDefinition = "JSONB")
    private String stages;

    @Column(name = "on_hold", nullable = false)
    private Boolean onHold;
}
