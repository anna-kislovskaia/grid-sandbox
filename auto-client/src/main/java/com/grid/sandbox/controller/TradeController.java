package com.grid.sandbox.controller;

import com.grid.sandbox.model.PageUpdate;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.service.TradeReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/trades")
public class TradeController {
    private static final Predicate<Trade> OPEN_TRADES = trade -> !trade.getStatus().isFinal();


    @Autowired
    private TradeReportService tradeReportService;

    @GetMapping(path = "/open", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<PageUpdate<Trade>> getOpenTrades(@RequestParam Optional<Integer> page,
                                                 @RequestParam Optional<Integer> size,
                                                 @RequestParam Optional<String> sort)
    {
        Pageable request = getPagebleRequest(page, size, sort);
        return Flux.from(tradeReportService.getTrades(request, OPEN_TRADES));
    }

    @GetMapping(path = "/all", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<PageUpdate<Trade>> getAllTrades(@RequestParam Optional<Integer> page,
                                                 @RequestParam Optional<Integer> size,
                                                 @RequestParam Optional<String> sort)
    {
        Pageable request = getPagebleRequest(page, size, sort);
        return Flux.from(tradeReportService.getTrades(request, (trade) -> true));
    }

    public static Pageable getPagebleRequest(Optional<Integer> page, Optional<Integer> size, Optional<String> sortSettings) {
        Sort sort = parseSort(sortSettings);
        if (page.isPresent() || size.isPresent()) {
            return PageRequest.of(page.orElse(0), size.orElse(100), sort);
        } else {
            return new UnpagedRequest(sort);
        }
    }

    public static Sort parseSort(Optional<String> sortSettings) {
        return sortSettings.map(sorting -> {
            List<Sort.Order> orders = Arrays.stream(sorting.split(","))
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
            return Sort.by(orders);
        }).orElse(Sort.unsorted());
    }
}
