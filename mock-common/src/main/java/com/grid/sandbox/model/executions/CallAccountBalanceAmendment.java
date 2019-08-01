package com.grid.sandbox.model.executions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class CallAccountBalanceAmendment implements CallAccountExecution {
    @Getter
    private String accountId;
    @Getter
    private long version;
    @Getter
    private long timestamp;
    @Getter
    private BigDecimal amount;

}
