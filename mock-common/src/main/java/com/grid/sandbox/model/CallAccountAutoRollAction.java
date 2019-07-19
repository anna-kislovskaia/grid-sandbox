package com.grid.sandbox.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@AllArgsConstructor
@Data
public class CallAccountAutoRollAction implements CallAccountAction {
    @Getter
    private final String accountId;
    @Getter
    private final boolean autoroll;

    @Override
    public Type getType() {
        return Type.AUTO_ROLL;
    }

}
