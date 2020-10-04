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
        tradeComparators.put("lastUpdateTimestamp", Comparator.comparingLong(Trade::getLastUpdateTimestamp));
        tradeComparators.put("client", Comparator.comparing(Trade::getClient));
        tradeComparators.put("balance", Comparator.comparing(Trade::getBalance));
        tradeComparators.put("status", Comparator.comparing(Trade::getStatus));
    }


    public Comparator<Trade> getTradePropertyComparator(String property) {
        return tradeComparators.get(property);
    }
}
