package com.grid.sandbox.model;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.math.BigDecimal;
import java.util.List;

@Data
public class CallAccount {
    @Getter @NonNull
    private String accountId;

    @Getter
    private boolean autoRoll;

    @Getter
    private BigDecimal balance;

    private long version;

    @Getter
    private List<CallAccountAction> actions;

}

