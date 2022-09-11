package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterViewport;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.core.utils.MultiComparator;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.utils.TestHelpers;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;

import static com.grid.sandbox.utils.CacheUtils.ACCEPT_OPENED;
import static com.grid.sandbox.utils.TestHelpers.ID_COMPARATOR;

@ExtendWith(MockitoExtension.class)
@Log4j2
class PerformanceBlotterReportServiceTest {
     @Test
    @Disabled
    void testSnapshotNoComparators() throws Throwable {
        blotterReportTest(200_000, 300, ID_COMPARATOR);
    }

    @Test
    @Disabled
    void testSnapshotNoComparatorsAsMulti() throws Throwable {
        blotterReportTest(200_000, 300, new MultiComparator<>(Collections.singletonList(ID_COMPARATOR)));
    }

    @Test
    @Disabled
    void testSnapshotComparators() throws Throwable {
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        blotterReportTest(500_000, 10, comparator);
    }

    void blotterReportTest(int itemCount, int subscriptionCount, Comparator<Trade> comparator) throws Throwable {
        log.info("Generating snapshot");
        List<UpdateEventEntry<String, Trade>> trades = IntStream.range(0, itemCount)
                .mapToObj((i) -> TestHelpers.generateNewTrade())
                .map(UpdateEventEntry::addedValue)
                .toList();

        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(trades, UpdateEvent.Type.SNAPSHOT)
        );
        log.info("generated [{}]", itemCount);
        Flowable<BlotterViewport> request = Flowable.just(new BlotterViewport(0, 1000));
        DoubleAdder totalSubscriptionTime = new DoubleAdder();
        DoubleAdder totalPageUpdateTime = new DoubleAdder();
        CountDownLatch latch = new CountDownLatch(subscriptionCount);
        IntStream.range(0, subscriptionCount).parallel().forEach(i -> {
            long start = System.currentTimeMillis();
            BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_OPENED, comparator);
            blotterReportService.getReport(feed, request).subscribeOn(Schedulers.computation()).subscribe((pageUpdate) -> {
                long processingTime = System.currentTimeMillis() - start;
                totalPageUpdateTime.add(processingTime);
                log.info("First page time {}: {}", i, processingTime);
                latch.countDown();
            });
            long processingTime = System.currentTimeMillis() - start;
            totalSubscriptionTime.add(processingTime);
            log.info("Subscription time {}: {}", i, processingTime);
        });
        latch.await();
        log.info("Average subscription time {}", totalSubscriptionTime.doubleValue() / subscriptionCount);
        log.info("Average first page time {}", totalPageUpdateTime.doubleValue() / subscriptionCount);
    }
}
