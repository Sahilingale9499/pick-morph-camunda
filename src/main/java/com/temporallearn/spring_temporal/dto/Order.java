package com.temporallearn.spring_temporal.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId; // This is the pickinstruction id
    private List<OrderLine> orderLines;
}