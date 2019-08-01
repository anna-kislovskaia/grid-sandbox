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

    public long getVersion() {
        return value.getVersion();
    }

    public String getAccountId() {
        return value.getAccountId();
    }

    public CallAccountUpdate merge(CallAccountUpdate existing) {
        if (existing == null) {
            return this;
        }
        if (this.getVersion() > existing.getVersion()) {
            return new CallAccountUpdate(value, existing.getValue());
        } else {
            return existing;
        }
    }
}
