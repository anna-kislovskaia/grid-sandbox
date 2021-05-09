package com.grid.sandbox.service;

import com.grid.sandbox.core.model.PageUpdate;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.utils.ReportSubscription;
import com.grid.sandbox.core.service.BlotterReportService;
import com.grid.sandbox.core.service.FilterOptionService;
import com.grid.sandbox.core.utils.MultiComparator;
import com.grid.sandbox.core.utils.MultiPredicate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.utils.*;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@Service
@Log4j2
public class TradeReportService {

    @Autowired
    private TradeFeedService tradeFeedService;

    @Autowired
    private ComparatorsService comparatorsService;


    public Flowable<PageUpdate<Trade>> getTrades(ReportSubscription<Trade> subscription, Predicate<Trade> reportFilter) {
        String feedId = "tradeFeed-" + subscription.getSubscriptionId();
        log.info("{} Subscription requested", feedId);
        Scheduler scheduler = subscription.getScheduler();
        FilterOptionService<String, Trade> filterOptionService = new FilterOptionService<>(
                tradeFeedService.getTradeFeed(scheduler),
                tradeFeedService.getTradeSnapshotFeed(),
                CacheUtils.getTradeFilterOptionBuilder(),
                reportFilter
        );

        Flowable<Predicate<Trade>> compositeFilterFeed = subscription.getUserFilterFeed().map(userFilter ->
                userFilter == null ? reportFilter : new MultiPredicate<>(Arrays.asList(reportFilter, userFilter))
        );
        Flowable<PageUpdate<Trade>> valueFeed = Flowable
                .combineLatest(subscription.getRequestFeed(), compositeFilterFeed, RequestParams::new)
                .throttleLast(1000, TimeUnit.MILLISECONDS)
                .switchMap(params -> {
                    Comparator<Trade> comparator = getTradeComparatorBySort(params.request.getSort());
                    Flowable<UpdateEvent<String, Trade>> tradeFeed = tradeFeedService.getTradeFeed(scheduler);
                    BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>();
                    return blotterReportService.getReport(tradeFeed, params.request, params.filter, comparator);
                });

        return Flowable.combineLatest(
                filterOptionService.getFilterOptions(),
                valueFeed,
                (filters, pageUpdate) -> pageUpdate.toBuilder()
                        .filterOptions(filters)
                        .subscriptionId(subscription.getSubscriptionId())
                        .build()
        )
                .doOnCancel(() -> log.info("{} Subscription cancelled", feedId))
                .doOnError(log::error);

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
