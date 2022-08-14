package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterViewport;
import com.grid.sandbox.core.utils.ReportSubscription;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.service.ComparatorsService;
import com.grid.sandbox.service.TradeReportService;
import com.grid.sandbox.utils.TestHelpers;
import io.reactivex.disposables.Disposable;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;

import static com.grid.sandbox.utils.CacheUtils.ACCEPT_ALL;
import static com.grid.sandbox.utils.CacheUtils.ACCEPT_OPENED;

@ExtendWith(MockitoExtension.class)
@Log4j2
class PerformanceTradeReportServiceTest {

    private final ComparatorsService comparatorsService = new ComparatorsService();
    private final KeyOrderedSchedulerService schedulerService = new KeyOrderedSchedulerService();
    private final BlotterFeedService<String, Trade> feedService = new BlotterFeedService<>();

    {
        feedService.init();
        schedulerService.init();
        comparatorsService.init();
    }

    @Test
    @Disabled
    void testSnapshotNoComparators() throws Throwable {
        blotterReportTest(200_000, 300,  Sort.unsorted());
    }

    @Test
    @Disabled
    void testSnapshotComparators() throws Throwable {
        Sort sort = Sort.by(Sort.Direction.ASC, "client", "balance");
        blotterReportTest(300000, 10, sort);
    }

    void blotterReportTest(int itemCount, int subscriptionCount, Sort sort) throws Throwable {
        log.info("Generating snapshot");
        List<Trade> trades = IntStream.range(0, itemCount)
                .mapToObj((i) -> TestHelpers.generateNewTrade())
                .toList();
        feedService.reset(trades);

        log.info("generated [{}]", itemCount);
        BlotterViewport viewport = new BlotterViewport(0, 1000);
        DoubleAdder totalSubscriptionTime = new DoubleAdder();
        DoubleAdder totalPageUpdateTime = new DoubleAdder();
        CountDownLatch latch = new CountDownLatch(subscriptionCount);
        IntStream.range(0, subscriptionCount).parallel().forEach(i -> {
            ReportSubscription subscription = new ReportSubscription(i, viewport, sort, schedulerService.getScheduler("feed-" + i));
            long start = System.currentTimeMillis();
            TradeReportService tradeReportService = new TradeReportService(feedService, comparatorsService);
            AtomicReference<Disposable> subscrRef = new AtomicReference<>();
            Disposable tradeSubscription = tradeReportService.getTrades(subscription, ACCEPT_OPENED)
                    .subscribe((pageUpdate) -> {
                        long processingTime = System.currentTimeMillis() - start;
                        totalPageUpdateTime.add(processingTime);
                        log.info("{}: First page time: {}", "tradeFeed-" + i, processingTime);
                        Disposable sub = subscrRef.get();
                        if (sub != null) {
                            sub.dispose();
                        } else {
                            log.warn("{}: Cannot close subscription", "tradeFeed-" + i);
                        }
                        latch.countDown();
                    });
            subscrRef.set(tradeSubscription);
            long processingTime = System.currentTimeMillis() - start;
            totalSubscriptionTime.add(processingTime);
            log.info("Subscription time {}: {}", i, processingTime);
        });
        latch.await();
        log.info("Average subscription time {}", totalSubscriptionTime.doubleValue() / subscriptionCount);
        log.info("Average first page time {}", totalPageUpdateTime.doubleValue() / subscriptionCount);
    }
}
