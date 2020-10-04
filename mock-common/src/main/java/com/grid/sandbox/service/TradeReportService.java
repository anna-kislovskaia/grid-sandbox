package com.grid.sandbox.service;

import com.grid.sandbox.model.PageUpdate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.UpdateEvent;
import com.grid.sandbox.utils.CacheUtils;
import com.grid.sandbox.utils.RedBlackBST;
import com.grid.sandbox.utils.MultiComparator;
import com.hazelcast.core.EntryEvent;
import io.reactivex.Flowable;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Log4j2
public class TradeReportService {

    @Autowired
    private TradeFeedService tradeFeedService;

    @Autowired
    private ComparatorsService comparatorsService;

    public Flowable<PageUpdate<Trade>> getTrades(Pageable request, Predicate<Trade> filter) {
        Comparator<Trade> comparator = getTradeComparatorBySort(request.getSort());
        final RedBlackBST<Trade, Trade> sortedTrades = new RedBlackBST<>(comparator);
        final AtomicReference<Map<String, Trade>> page = new AtomicReference<>(new HashMap<>());
        return tradeFeedService.getTradeFeed()
                .map(updateEvent -> {
                    synchronized (sortedTrades) {
                        Map<String, Trade> changed = handleTradeUpdates(updateEvent, filter, sortedTrades);
                        boolean snapshot = updateEvent.getType() == UpdateEvent.Type.SNAPSHOT;
                        PageUpdate.Builder<Trade> builder = PageUpdate.<Trade>builder()
                                .totalSize(sortedTrades.size())
                                .pageSize(request.getPageSize())
                                .pageNumber(request.getPageNumber());

                        if (request.isPaged()) {
                            handlePagedUpdate(request, page, builder, snapshot, changed, sortedTrades);
                        } else {
                            handleUnpagedUpdate(builder, snapshot, changed, sortedTrades);
                        }
                        return builder.build();
                    }
                })
                .filter(update -> !update.isEmpty() || update.isSnapshot());
    }

    private static void handlePagedUpdate(Pageable request,
                                          AtomicReference<Map<String, Trade>> page,
                                          PageUpdate.Builder<Trade> builder,
                                          boolean snapshot,
                                          Map<String, Trade> changed,
                                          RedBlackBST<Trade, Trade> sortedTrades) {

        if (request.getOffset() >= sortedTrades.size()) {
            Map<String, Trade> old = page.getAndSet(Collections.emptyMap());
            builder.updated(Collections.emptyList())
                    .deleted(new ArrayList<>(old.values()));
            return;
        }

        long minRank = request.getOffset();
        long maxRank = minRank + request.getPageSize() - 1;

        Trade min = sortedTrades.select((int)minRank);
        Trade max = maxRank >= sortedTrades.size() ? sortedTrades.max() : sortedTrades.select((int)maxRank);
        Iterable<Trade> pageContent = sortedTrades.keys(min, max);
        Map<String, Trade> current = new LinkedHashMap<>();
        for (Trade existing: pageContent) {
            current.put(existing.getTradeId(), existing);
        }

        Map<String, Trade> old = page.getAndSet(current);
        if (old.isEmpty() || snapshot) {
            builder.snapshot(true)
                    .updated(new ArrayList<>(current.values()))
                    .deleted(Collections.emptyList());
        } else {
            List<Trade> updated = current.values().stream()
                    .filter(trade -> !old.containsKey(trade.getTradeId()) || changed.containsKey(trade.getTradeId()))
                    .collect(Collectors.toList());
            old.keySet().removeAll(current.keySet());
            builder.updated(updated).deleted(new ArrayList<>(old.values()));
        }
    }

    private static void handleUnpagedUpdate(PageUpdate.Builder<Trade> builder,
                                            boolean snapshot,
                                            Map<String, Trade> updated,
                                            RedBlackBST<Trade, Trade> sortedTrades) {
        if (snapshot) {
            builder.snapshot(true)
                    .updated(toList(sortedTrades.keys()))
                    .deleted(Collections.emptyList());
        } else {
            Map<Boolean, List<Trade>> partitioned = updated.values().stream()
                    .collect(Collectors.partitioningBy(sortedTrades::contains));
            builder.updated(partitioned.get(true))
                    .deleted(partitioned.get(false));
        }
    }

    private static <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        for (T value : iterable) {
            list.add(value);
        }
        return list;
    }

    private MultiComparator<Trade> getTradeComparatorBySort(Sort sort) {
        List<Comparator<Trade>> comparators = new ArrayList<>();
        sort.forEach(order -> {
            Comparator<Trade> comparator = comparatorsService.getTradePropertyComparator(order.getProperty());
            if (comparator != null) {
                comparator = order.getDirection() == Sort.Direction.DESC ? comparator.reversed() : comparator;
                comparators.add(comparator);
            } else {
                log.error("Trade comparator for property {} is not found", order.getProperty());
            }
        });
        // unique comparator
        comparators.add(CacheUtils.TRADE_ID_COMPARATOR);
        return new MultiComparator<>(comparators);
    }

    private static Map<String, Trade> handleTradeUpdates(UpdateEvent updateEvent, Predicate<Trade> filter, RedBlackBST<Trade, Trade> trades) {
        Map<String, Trade> updated = new HashMap<>();
        boolean snapshot = updateEvent.getType() == UpdateEvent.Type.SNAPSHOT;
        if (snapshot) {
            trades.clear();
        }
        log.info("Processing trade update: {} {}", snapshot, updateEvent.getUpdates().size());
        for (EntryEvent<String, Trade> event : updateEvent.getUpdates().values()) {
            boolean tradeDeleted = event.getOldValue() != null && trades.delete(event.getOldValue()) != null;
            Trade trade = event.getValue();
            boolean tradeUpdated = trade != null && filter.test(trade);
            if (tradeUpdated) {
                trades.put(trade, trade);
            }
            if ((tradeDeleted || tradeUpdated) && !snapshot) {
                Trade changed = tradeUpdated ? trade : event.getOldValue();
                updated.put(changed.getTradeId(), changed);
            }
        }
        log.info("Processed");
        return updated;
    }


}
