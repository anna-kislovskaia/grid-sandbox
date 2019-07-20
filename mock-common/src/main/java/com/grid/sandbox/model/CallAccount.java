package com.grid.sandbox.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class CallAccount implements Serializable {
    private final String accountId;
    private final BigDecimal balance;
    private boolean autoRoll;
    private long version;

    public CallAccount(String accountId) {
        this.accountId = accountId;
        this.balance = BigDecimal.ZERO;
    }

    public String getAccountId() {
        return accountId;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public boolean isAutoRoll() {
        return autoRoll;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "CallAccount{" +
                "accountId='" + accountId + '\'' +
                ", autoRoll=" + autoRoll +
                ", balance=" + balance +
                ", version=" + version +
                '}';
    }
}

