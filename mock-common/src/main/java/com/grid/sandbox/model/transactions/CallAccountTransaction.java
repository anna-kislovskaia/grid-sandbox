package com.grid.sandbox.model.transactions;

import java.io.Serializable;

public interface CallAccountTransaction extends Serializable {
    String getAccountId();
    long getVersion();
    long getTimestamp();
}
