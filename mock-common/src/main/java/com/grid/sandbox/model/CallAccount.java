package com.grid.sandbox.model;

import com.grid.sandbox.model.actions.CallAccountAction;
import com.grid.sandbox.model.actions.CallAccountBalanceAction;
import com.grid.sandbox.model.transactions.CallAccountBalanceAmendment;
import com.grid.sandbox.model.transactions.CallAccountStaticsAmendment;
import com.grid.sandbox.model.transactions.CallAccountTransaction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CallAccount implements Serializable {
    private final String accountId;
    private final BigDecimal balance;
    private final boolean autoRoll;
    private final long version;
    private final List<CallAccountAction> actions;
    private final List<CallAccountTransaction> transactions;

    public CallAccount(String accountId) {
        this.accountId = accountId;
        this.balance = BigDecimal.ZERO;
        this.transactions  = Collections.emptyList();
        this.actions  = Collections.emptyList();
        this.version = 0;
        this.autoRoll = false;
    }

    public CallAccount(String accountId,
                       BigDecimal balance,
                       boolean autoRoll,
                       long version,
                       List<CallAccountAction> actions,
                       List<CallAccountTransaction> transactions)
    {
        this.accountId = accountId;
        this.balance = balance;
        this.autoRoll = autoRoll;
        this.version = version;
        this.actions = Collections.unmodifiableList(actions);
        this.transactions = Collections.unmodifiableList(transactions);
    }

    public String getAccountId() {
        return accountId;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public boolean isAutoRoll() {
        return autoRoll;
    }

    public long getVersion() {
        return version;
    }

    public List<CallAccountAction> getActions() {
        return actions;
    }

    public List<CallAccountTransaction> getTransactions() {
        return transactions;
    }

    public CallAccount apply(CallAccountAction action) {
        long nextVersion = version + 1;
        List<CallAccountAction> nextActions = new ArrayList<>(actions);
        nextActions.add(action);
        List<CallAccountTransaction> nextTransactions = new ArrayList<>(transactions);
        long timestamp = System.currentTimeMillis();
        switch (action.getType()) {
            case INCREASE:
            case WITHDRAW:
                CallAccountBalanceAction balanceAction = (CallAccountBalanceAction)action;
                BigDecimal amount = action.getType() == CallAccountAction.Type.WITHDRAW ?
                        balanceAction.getAmount().abs().negate() : balanceAction.getAmount().abs();
                BigDecimal resultAmount = this.balance.add(amount);
                nextTransactions.add(new CallAccountBalanceAmendment(accountId, nextVersion, timestamp, amount));
                return new CallAccount(accountId, resultAmount, this.autoRoll, nextVersion, nextActions, nextTransactions);
            case AUTO_ROLL:
                boolean autorollStatus = ((CallAccountStaticsAmendment)action).isAutoroll();
                if (this.autoRoll != autorollStatus) {
                    nextTransactions.add(new CallAccountStaticsAmendment(accountId, nextVersion, timestamp, autorollStatus));
                }
                return new CallAccount(accountId, balance, ((CallAccountStaticsAmendment)action).isAutoroll(), nextVersion, nextActions, nextTransactions);
        }
        throw new UnsupportedOperationException("Unrecognized action type " + action.getType());
    }

    @Override
    public String toString() {
        return "CallAccount{" +
                "accountId='" + accountId + '\'' +
                ", autoRoll=" + autoRoll +
                ", balance=" + balance +
                ", version=" + version +
                '}';
    }
}

