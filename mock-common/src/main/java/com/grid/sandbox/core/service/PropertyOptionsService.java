package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.*;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

@AllArgsConstructor
@Log4j2
public class PropertyOptionsService<K, V extends BlotterReportRecord<K>> {
    private String feedId;
    private BlotterFeedService<K, V> blotterFeedService;
    private Scheduler scheduler;
    private PropertyOptionsTracker<V> optionsTracker;
    private Predicate<V> filter;
    private final AtomicReference<Disposable> subscription = new AtomicReference<>();

    public void subscribe() {
        close();
        Disposable filterSubscription = blotterFeedService.getFeed(feedId, scheduler)
                .filter(this::filterOptionsMightChange)
                .map(event -> {
                    log.info("{}: Recalculate property options start", feedId);
                    UpdateEvent<K, V> snapshot = event.isSnapshot() ? event : blotterFeedService.getSnapshot();
                    Stream<V> valueStream = snapshot.getUpdates().stream().map(UpdateEventEntry::getValue).filter(filter);
                    optionsTracker.resetFilterOptions(valueStream);
                    log.info("{}: Recalculate property options done", feedId);
                    return optionsTracker.getFilterOptions();
                })
                .doOnCancel(() -> {
                    log.info("{}: Property option tracker closed", feedId);
                })
                .subscribe();
        if (!subscription.compareAndSet(null, filterSubscription)) {
            filterSubscription.dispose();
        }
    }

    public void close() {
        Disposable current = subscription.get();
        if (subscription.compareAndSet(current, null) && current != null) {
            current.dispose();
        }
    }

    public List<PropertyOptionsUpdateEntry> getFilterOptions() {
        return new ArrayList<>(optionsTracker.getFilterOptions());
    }

    private boolean filterOptionsMightChange(UpdateEvent<K, V> event) {
        Collection<PropertyOptionsUpdateEntry> currentOptions = optionsTracker.getFilterOptions();
        return event.getType() == UpdateEvent.Type.SNAPSHOT ||
                currentOptions.isEmpty() ||
                event.getUpdates().stream().anyMatch(entry -> {
                    V value = entry.getValue();
                    // value deleted
                    if (entry.getOldValue() != null && filter.test(entry.getOldValue()) && !filter.test(value)) {
                        return true;
                    }
                    // options extended
                    return filter.test(value) && optionsTracker.filtersChanged(value);
                });
    }

}
