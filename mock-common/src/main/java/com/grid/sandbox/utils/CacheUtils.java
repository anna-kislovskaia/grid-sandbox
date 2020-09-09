package com.grid.sandbox.utils;

import com.grid.sandbox.model.Trade;

import java.util.Comparator;

public class CacheUtils {
    public static final String TRADE_CACHE = "Trades";
    public static final int CALL_ACCOUNT_COUNT = 1000;


    public static Comparator<Trade> TRADE_ID_COMPARATOR =
            (trade1, trade2) -> {
                String[] items1 = trade1.getTradeId().split("\\.");
                String[] items2 = trade2.getTradeId().split("\\.");
                int result = items1[0].compareTo(items2[0]);
                if (result == 0) {
                    return Integer.valueOf(items1[1]) - Integer.valueOf(items2[1]);
                }
                return result;
            };

}
