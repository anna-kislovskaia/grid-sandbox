package com.grid.sandbox.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.grid.sandbox.core.model.BlotterReportRecord;
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
public class Trade implements Serializable, BlotterReportRecord<String> {
    @JsonProperty
    private String tradeId;
    @JsonProperty
    private BigDecimal balance;
    @JsonProperty
    private String client;
    @JsonProperty
    private long lastUpdateTimestamp;
    @JsonProperty
    private TradeStatus status;

    @Override
    public String getRecordKey() {
        return tradeId;
    }

    @Override
    public long getRecordVersion() {
        return lastUpdateTimestamp;
    }
}

