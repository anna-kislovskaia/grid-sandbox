package com.grid.sandbox.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;

@AllArgsConstructor
@ToString
@Getter
@Builder(toBuilder = true, builderClassName = "Builder")
public class Trade implements Serializable {
    private String tradeId;
    private BigDecimal balance;
    private String client;
    private long lastUpdateTimestamp;
    private TradeStatus status;
}

