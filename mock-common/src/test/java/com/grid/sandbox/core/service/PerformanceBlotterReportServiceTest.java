package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterViewport;
import com.grid.sandbox.core.model.PageUpdate;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.core.utils.MultiComparator;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.utils.TestHelpers;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.grid.sandbox.utils.CacheUtils.ACCEPT_ALL;
import static com.grid.sandbox.utils.TestHelpers.ID_COMPARATOR;

@ExtendWith(MockitoExtension.class)
@Log4j2
class PerformanceBlotterReportServiceTest {
    @Mock
    private Consumer<PageUpdate> consumer;

    @Test
    @Disabled
    void testSnapshotNoComparators() throws Throwable {
        runTest(200_000, 300, ID_COMPARATOR);
    }

    @Test
    @Disabled
    void testSnapshotNoComparatorsAsMulti() throws Throwable {
        runTest(200_000, 300, new MultiComparator<>(Collections.singletonList(ID_COMPARATOR)));
    }

    @Test
    @Disabled
    void testSnapshotComparators() throws Throwable {
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        runTest(200_000, 500, comparator);
    }

    void runTest(int itemCount, int retryCount, Comparator<Trade> comparator) throws Throwable {
        log.info("Generating snapshot");
        List<UpdateEventEntry<String, Trade>> trades = IntStream.range(0, itemCount)
                .mapToObj((i) -> TestHelpers.generateNewTrade())
                .map(UpdateEventEntry::addedValue)
                .collect(Collectors.toList());

        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(trades, UpdateEvent.Type.SNAPSHOT)
        );
        log.info("generated");
        Flowable<BlotterViewport> request = Flowable.just(new BlotterViewport(0, 1000));
        long totalTime = 0;
        for (int i = 1; i < retryCount; i++) {
            long start = System.currentTimeMillis();
            BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_ALL, comparator);
            blotterReportService.getReport(feed, request).subscribe(consumer);
            long processingTime = System.currentTimeMillis() - start;
            log.info("Processing time: {}", processingTime);
            totalTime += processingTime;
        }
        log.info("Average processing time {}", ((double) totalTime) / retryCount);
    }
}
