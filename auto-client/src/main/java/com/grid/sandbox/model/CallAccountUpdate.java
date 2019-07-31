package com.grid.sandbox.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@AllArgsConstructor
public class CallAccountUpdate {
    @Getter
    private final CallAccount oldValue;
    @Getter
    private final CallAccount value;

    public long getOldVersion() {
        return oldValue != null ? oldValue.getVersion() : 0;
    }

    public long getVersion() {
        return value.getVersion();
    }

    public String getAccountId() {
        return value.getAccountId();
    }

    public CallAccountUpdate merge(CallAccountUpdate another) {
        if (another == null) {
            return this;
        }
        return new CallAccountUpdate(
                this.getOldVersion() < another.getOldVersion() ? oldValue : another.getOldValue(),
                this.getVersion() > another.getVersion() ? value : another.getValue()
        );
    }
}
