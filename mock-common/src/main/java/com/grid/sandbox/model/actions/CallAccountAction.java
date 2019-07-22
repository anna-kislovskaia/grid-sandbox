package com.grid.sandbox.model.actions;

import java.io.Serializable;

public interface CallAccountAction extends Serializable {
    enum Type {
        INCREASE, WITHDRAW, AUTO_ROLL
    }

    Type getType();

    String getActionId();

    String getAccountId();

    long getTimestamp();
}
