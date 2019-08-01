package com.grid.sandbox.model.executions;

import java.io.Serializable;

public interface CallAccountExecution extends Serializable {
    String getAccountId();
    long getVersion();
    long getTimestamp();
}
