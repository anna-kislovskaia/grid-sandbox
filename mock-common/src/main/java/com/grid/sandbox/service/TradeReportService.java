package com.grid.sandbox.service;

import com.grid.sandbox.model.PageUpdate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.utils.*;
import io.reactivex.Flowable;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;

import static com.grid.sandbox.utils.CacheUtils.TRADE_KEY_MAPPER;

@Service
@Log4j2
public class TradeReportService {

    @Autowired
    private TradeFeedService tradeFeedService;

    @Autowired
    private ComparatorsService comparatorsService;


    public Flowable<PageUpdate<Trade>> getTrades(ReportSubscription<Trade> subscription, Predicate<Trade> reportFilter) {
        FilterOptionService<String, Trade> filterOptionService = new FilterOptionService<>(
                tradeFeedService.getTradeFeed(), CacheUtils.getTradeFilterOptionBuilder(), reportFilter, subscription.getScheduler());
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(
                tradeFeedService.getTradeFeed(), TRADE_KEY_MAPPER, subscription.getScheduler());

        Flowable<Predicate<Trade>> compositeFilterFeed = subscription.getUserFilterFeed().map(userFilter ->
                userFilter == null ? reportFilter : new MultiPredicate<>(Arrays.asList(reportFilter, userFilter))
        );
        Flowable<PageUpdate<Trade>> valueFeed = Flowable
                .combineLatest(subscription.getRequestFeed(), compositeFilterFeed, RequestParams::new)
                .switchMap(params -> {
                    Comparator<Trade> comparator = getTradeComparatorBySort(params.request.getSort());
                    return blotterReportService.getReport(params.request, params.filter, comparator);
                });

        return Flowable.combineLatest(
                filterOptionService.getFilterOptions(),
                valueFeed,
                (filters, pageUpdate) -> pageUpdate.toBuilder()
                        .filterOptions(filters)
                        .subscriptionId(subscription.getSubscriptionId())
                        .build()
        );

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

    @AllArgsConstructor
    private static class RequestParams {
        private Pageable request;
        private Predicate<Trade> filter;
    }

}