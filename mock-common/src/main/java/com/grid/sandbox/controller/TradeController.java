package com.grid.sandbox.controller;

import com.grid.sandbox.model.PageUpdate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.service.KeyOrderedSchedulerService;
import com.grid.sandbox.service.TradeReportService;
import io.reactivex.Scheduler;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.context.request.RequestContextHolder;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Log4j2
@Controller
@RequestMapping("/trades")
public class TradeController {
    private static final Predicate<Trade> OPEN_TRADES = trade -> !trade.getStatus().isFinal();


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
        return Flux.from(tradeReportService.getTrades(request, OPEN_TRADES, getSessionScheduler()));
    }

    @GetMapping(path = "/all", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<PageUpdate<Trade>> getAllTrades(@RequestParam Optional<Integer> page,
                                                 @RequestParam Optional<Integer> size,
                                                 @RequestParam Optional<List<String>> sort)
    {
        Pageable request = getPagebleRequest(page, size, parseSort(sort));
        return Flux.from(tradeReportService.getTrades(request, (trade) -> true, getSessionScheduler()));
    }

    public static Pageable getPagebleRequest(Optional<Integer> page, Optional<Integer> size, Sort sort) {
        if (page.isPresent() || size.isPresent()) {
            return PageRequest.of(page.orElse(0), size.orElse(100), sort);
        } else {
            return new UnpagedRequest(sort);
        }
    }

    public static Sort parseSort(Optional<List<String>> sortSettings) {
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
