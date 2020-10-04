package com.grid.sandbox.service;

import com.grid.sandbox.model.Trade;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@Service
public class ComparatorsService {
    private final Map<String, Comparator<Trade>> tradeComparators = new HashMap<>();

    @PostConstruct
    public void init() {
        tradeComparators.put("lastUpdateTimestamp",
                (trade1, trade2) -> Long.compare(trade1.getLastUpdateTimestamp(), trade2.getLastUpdateTimestamp()));
        tradeComparators.put("client",
                (trade1, trade2) -> trade1.getClient().compareTo(trade2.getClient()));
        tradeComparators.put("balance",
                (trade1, trade2) -> trade1.getBalance().compareTo(trade2.getBalance()));
        tradeComparators.put("status",
                (trade1, trade2) -> trade1.getStatus().compareTo(trade2.getStatus()));
    }


    public Comparator<Trade> getTradePropertyComparator(String property) {
        return tradeComparators.get(property);
    }
}
