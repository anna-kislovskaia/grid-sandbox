package com.grid.sandbox.model.actions;

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
    @Getter
    private final String actionId;
    @Getter
    private final long timestamp;

    @Override
    public Type getType() {
        return Type.AUTO_ROLL;
    }

}
