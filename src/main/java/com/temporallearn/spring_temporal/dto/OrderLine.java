package com.temporallearn.spring_temporal.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderLine {
    // Details of pickinstruction
    private String pickInstructionId;
    private String item;
    private String tpid;
    private String uom;
    private int qty;
    
    // Inventory/Warehouse details
    private List<String> scannableBarcodes;
    private String pickLocation;
    private String dropLocation;
}