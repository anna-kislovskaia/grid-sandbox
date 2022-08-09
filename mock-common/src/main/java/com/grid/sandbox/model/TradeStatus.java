package com.grid.sandbox.model;

public enum TradeStatus {
    DRAFT, PLACED, REJECTED, CANCELLED;

    public boolean isFinal() {
        return this == REJECTED || this == CANCELLED;
    }

}
