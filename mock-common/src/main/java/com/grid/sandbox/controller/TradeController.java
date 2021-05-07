package com.grid.sandbox.controller;

import com.grid.sandbox.core.model.PageUpdate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.core.service.KeyOrderedSchedulerService;
import com.grid.sandbox.core.utils.ReportSubscription;
import com.grid.sandbox.service.TradeReportService;
import com.grid.sandbox.core.utils.MultiPredicate;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.context.request.RequestContextHolder;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.*;

@Log4j2
@Controller
@RequestMapping("/trades")
public class TradeController {
    private final AtomicInteger subscriptionIdGenerator = new AtomicInteger();
    private final ConcurrentMap<Integer, ReportSubscription<Trade>> subscriptions = new ConcurrentHashMap<>();

    @Autowired
    private TradeReportService tradeReportService;

    @Autowired
    private KeyOrderedSchedulerService keyOrderedSchedulerService;

    @GetMapping(path = "/open", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<PageUpdate<Trade>> getOpenTrades(@RequestParam Optional<Integer> page,
                                                 @RequestParam Optional<Integer> size,
                                                 @RequestParam Optional<List<String>> sort)
    {
        Pageable request = getPagebleRequest(page, size, parseSort(sort));
        ReportSubscription<Trade> subscription = createSubscription(request);
        return toFlux(tradeReportService.getTrades(subscription, ACCEPT_OPENED), subscription);
    }


    @GetMapping(path = "/all", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<PageUpdate<Trade>> getAllTrades(@RequestParam Optional<Integer> page,
                                                 @RequestParam Optional<Integer> size,
                                                 @RequestParam Optional<List<String>> sort)
    {
        Pageable request = getPagebleRequest(page, size, parseSort(sort));
        ReportSubscription<Trade> subscription = createSubscription(request);
        return toFlux(tradeReportService.getTrades(subscription, ACCEPT_ALL), subscription);
    }

    private <T> Flux<T> toFlux(Flowable<T> feed, ReportSubscription<?> subscription) {
        Subscription[] subscriptionHandler = new Subscription[1];
        Flowable<T> cancellableFeed = feed.doOnSubscribe(feedSubscription -> subscriptionHandler[0] = feedSubscription);
        return Flux.from(cancellableFeed)
                .doOnCancel(() -> {
                    // flux does not stop underlying stream
                    subscriptionHandler[0].cancel();
                    closeSubscription(subscription.getSubscriptionId());
                });
    }

    @PutMapping(path = "/params")
    public ResponseEntity updateSubscription(@RequestParam int subscriptionId,
                                             @RequestParam Optional<Integer> page,
                                             @RequestParam Optional<Integer> size,
                                             @RequestParam Optional<List<String>> sort,
                                             @RequestParam Optional<String> filter) {

        Pageable request = getPagebleRequest(page, size, parseSort(sort));
        Optional<Predicate<Trade>> userFilter = filter.map(TradeController::parseOptionFilter);
        ReportSubscription<Trade> subscription = subscriptions.get(subscriptionId);
        if (subscription != null) {
            subscription.getPageableSubject().onNext(request);
            userFilter.ifPresent(tradePredicate -> subscription.getUserFilterSubject().onNext(tradePredicate));
        } else {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok().build();
    }

    private ReportSubscription<Trade> createSubscription(Pageable request) {
        ReportSubscription<Trade> subscription = new ReportSubscription<>(
                subscriptionIdGenerator.incrementAndGet(),
                request,
                ACCEPT_ALL,
                getSessionScheduler());
        subscriptions.put(subscription.getSubscriptionId(), subscription);
        return subscription;
    }

    private void closeSubscription(int subscriptionId) {
        ReportSubscription<Trade> subscription = subscriptions.remove(subscriptionId);
        log.info("Subscription {} {}", subscriptionId, subscription != null ? "closed" : " not found");
    }

    private static Pageable getPagebleRequest(Optional<Integer> page, Optional<Integer> size, Sort sort) {
        if (page.isPresent() || size.isPresent()) {
            return PageRequest.of(page.orElse(0), size.orElse(100), sort);
        } else {
            return new UnpagedRequest(sort);
        }
    }

    private static Predicate<Trade> parseOptionFilter(String filterStr) {
        if (filterStr.trim().isEmpty()) {
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

    private static Sort parseSort(Optional<List<String>> sortSettings) {
        log.info("Parse sort {}", sortSettings);
        return sortSettings.map(sorting -> {
            List<Sort.Order> orders = sorting.stream()
                    .map(orderSettings -> {
                        String[] settings = orderSettings.split(":");
                        if (settings.length == 2) {
                            Sort.Direction direction = Sort.Direction.fromString(settings[1]);
                            return new Sort.Order(direction, settings[0]);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            log.info("Sort parsed {}", orders);
            return Sort.by(orders);
        }).orElse(Sort.unsorted());
    }

    private Scheduler getSessionScheduler() {
        String sessionId = RequestContextHolder.currentRequestAttributes().getSessionId();
        return keyOrderedSchedulerService.getScheduler(sessionId);
    }

}
