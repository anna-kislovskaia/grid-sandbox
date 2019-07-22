package com.grid.sandbox.model.transactions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class CallAccountStaticsAmendment implements CallAccountTransaction {
    @Getter
    private String accountId;
    @Getter
    private long version;
    @Getter
    private long timestamp;
    @Getter
    private boolean autoroll;

}