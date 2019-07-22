package com.grid.sandbox.model.actions;

import lombok.Getter;

import java.math.BigDecimal;

public class CallAccountBalanceAction implements CallAccountAction {
    @Getter
    private final Type type;
    @Getter
    private final String accountId;
    @Getter
    private final BigDecimal amount;
    @Getter
    private final String actionId;
    @Getter
    private final long timestamp;

    public CallAccountBalanceAction(Type type, String accountId, BigDecimal amount, String actionId, long timestamp) {
        this.type = type;
        this.accountId = accountId;
        this.amount = amount;
        this.actionId = actionId;
        this.timestamp = timestamp;

        if (type != Type.INCREASE && type != Type.WITHDRAW) {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    @Override
    public String toString() {
        return "CallAccountBalanceAction{" +
                "type=" + type +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
