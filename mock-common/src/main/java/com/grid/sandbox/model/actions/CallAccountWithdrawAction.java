package com.grid.sandbox.model.actions;

import com.grid.sandbox.model.actions.CallAccountAction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.math.BigDecimal;

@AllArgsConstructor
@Data
public class CallAccountWithdrawAction implements CallAccountAction {
    @Getter
    private final String accountId;
    @Getter
    private final BigDecimal amount;
    @Getter
    private final long expectedAccountVersion;


    @Override
    public Type getType() {
        return Type.WITHDRAW;
    }
}
