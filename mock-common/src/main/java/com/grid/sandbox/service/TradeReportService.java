package com.grid.sandbox.service;

import com.grid.sandbox.core.model.PageUpdate;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.service.BlotterFeedService;
import com.grid.sandbox.core.utils.ReportSubscription;
import com.grid.sandbox.core.service.BlotterReportService;
import com.grid.sandbox.core.service.PropertyOptionsService;
import com.grid.sandbox.core.utils.MultiComparator;
import com.grid.sandbox.core.utils.MultiPredicate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.utils.*;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.*;

@Service
@AllArgsConstructor
@Log4j2
public class TradeReportService {

    @Autowired
    private final BlotterFeedService<String, Trade> blotterFeedService;

    @Autowired
    private final ComparatorsService comparatorsService;

    public Flowable<PageUpdate<Trade>> getTrades(ReportSubscription subscription, Predicate<Trade> reportFilter) {
        String feedId = "tradeFeed-" + subscription.getSubscriptionId();
        log.info("{} Subscription requested", feedId);
        Scheduler scheduler = subscription.getScheduler();
        PropertyOptionsService<String, Trade> filterOptionService = new PropertyOptionsService<>(
                feedId,
                blotterFeedService,
                scheduler,
                CacheUtils.getTradeFilterOptionBuilder(),
                reportFilter
        );
        filterOptionService.subscribe();

        Flowable<PageUpdate<Trade>> valueFeed = subscription.getUserSettingsFeed()
                .switchMap(params -> {
                    Comparator<Trade> comparator = getTradeComparatorBySort(params.getSort());
                    Predicate<Trade> userFilter = parseOptionFilter(params.getFilterString());
                    Predicate<Trade> filter = userFilter == null ? reportFilter :
                            new MultiPredicate<>(Arrays.asList(reportFilter, userFilter));
                    Flowable<UpdateEvent<String, Trade>> tradeFeed = blotterFeedService.getFeed(feedId, scheduler);
                    BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(filter, comparator);
                    return blotterReportService.getReport(tradeFeed, subscription.getViewportFeed());
                });

        return valueFeed.map(pageUpdate ->
                        pageUpdate.toBuilder()
                            .filterOptions(filterOptionService.getFilterOptions())
                            .subscriptionId(subscription.getSubscriptionId())
                            .build())
                .doOnCancel(() -> {
                    log.info("{} Subscription cancelled", feedId);
                    filterOptionService.close();
                })
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

    private static Predicate<Trade> parseOptionFilter(String filterStr) {
        if (filterStr == null || filterStr.trim().isEmpty()) {
            return ACCEPT_ALL;
        }
        log.info("Parsing option filters [{}]", filterStr);
        List<Predicate<Trade>> filters = Arrays.stream(filterStr.split(";")).map(filter -> {
            log.info("Parsing filter {}", filter);
            String[] parts = filter.split("=");
            String field = parts[0];
            Set<String> values = new HashSet<>(Arrays.asList(parts[1].split(",")));
            if (!values.isEmpty()) {
                if ("client".equals(field))
                    return getTradePredicate(TRADE_CLIENT_MAPPER, values);
                else if ("status".equals(field)) {
                    return getTradePredicate(TRADE_STATUS_MAPPER, values);
                }
            }
            throw new IllegalArgumentException("Invalid format: " + filter);
        }).collect(Collectors.toList());
        return filters.size() == 1 ? filters.get(0) : new MultiPredicate<>(filters);
    }

}
