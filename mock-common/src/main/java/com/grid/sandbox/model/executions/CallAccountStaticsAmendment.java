package com.grid.sandbox.model.executions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@AllArgsConstructor
public class CallAccountStaticsAmendment implements CallAccountExecution {
    @Getter
    private String accountId;
    @Getter
    private long version;
    @Getter
    private long timestamp;
    @Getter
    private boolean autoroll;

}
