package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.FilterOptionBuilder;
import com.grid.sandbox.core.model.FilterOptionUpdateEntry;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
@Log4j2
public class FilterOptionService<K, V> {
    private Flowable<UpdateEvent<K, V>> feed;
    private FilterOptionBuilder<V> filterBuilder;
    private Predicate<V> filter;
    private Scheduler scheduler;

    private final AtomicReference<Map<String, FilterOptionUpdateEntry>> lastOptions = new AtomicReference<>();

    public Flowable<List<FilterOptionUpdateEntry>> getFilterOptions() {
        return feed.filter(this::filterOptionsMightChange)
                .switchMap(event -> feed)
                .subscribeOn(scheduler)
                .filter(event -> event.getType() == UpdateEvent.Type.SNAPSHOT)
                .map(event -> {
                    log.info("Recalculate filter options");
                    Stream<V> valueStream = event.getUpdates().stream().map(UpdateEventEntry::getValue).filter(filter);
                    List<FilterOptionUpdateEntry> options = filterBuilder.getFilterOptions(valueStream);
                    Map<String, FilterOptionUpdateEntry> updatedOptions = options.stream()
                            .collect(Collectors.toMap(FilterOptionUpdateEntry::getName, entry -> entry));
                    lastOptions.set(updatedOptions);
                    return options;
                })
                .distinctUntilChanged();
    }

    private boolean filterOptionsMightChange(UpdateEvent<K, V> event) {
        Map<String, FilterOptionUpdateEntry> currentOptions = lastOptions.get();
        return event.getType() == UpdateEvent.Type.SNAPSHOT ||
                currentOptions == null ||
                event.getUpdates().stream().anyMatch(entry -> {
                    // value deleted
                    V value = entry.getValue();
                    if (value == null) {
                        return true;
                    }
                    // value does not match fitler
                    if (entry.getOldValue() != null && filter.test(entry.getOldValue()) && !filter.test(value)) {
                        return true;
                    }
                    // options extended
                    return filterBuilder.getFilterOptions(Stream.of(value)).stream().anyMatch(filterEntry ->
                            !currentOptions.containsKey(filterEntry.getName()) ||
                            !currentOptions.get(filterEntry.getName()).getOptions().containsAll(filterEntry.getOptions()));
                });
    }

}
