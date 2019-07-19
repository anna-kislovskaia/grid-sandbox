package com.grid.sandbox.model;

public interface CallAccountAction {
    enum Type {
        INCREASE, WITHDRAW, AUTO_ROLL
    }

    Type getType();
}
