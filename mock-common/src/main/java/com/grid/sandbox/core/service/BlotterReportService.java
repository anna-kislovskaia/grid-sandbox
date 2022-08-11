package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.*;
import com.grid.sandbox.core.utils.RedBlackBST;
import io.reactivex.Flowable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.grid.sandbox.core.model.UpdateEvent.getRecordVersion;

@Log4j2
@AllArgsConstructor
public class BlotterReportService<K, V extends BlotterReportRecord<K>> {
    private final Predicate<V> filter;
    private final Comparator<V> comparator;

    public Flowable<PageUpdate<V>> getReport(Flowable<UpdateEvent<K, V>> dataFeed, Flowable<BlotterViewport> viewportFeed) {
        final RedBlackBST<V, V> sortedValues = new RedBlackBST<>(comparator);
        final AtomicReference<Map<K, V>> page = new AtomicReference<>(new HashMap<>());
        final Set<K> keys = new HashSet<>();

        return Flowable.merge(
                dataFeed.map(event -> new ContentUpdateEvent<>(viewportFeed.blockingFirst(), event)),
                viewportFeed.map(viewport -> new ContentUpdateEvent<K, V>(viewport, null)))
                .map(event -> {
                    Map<K, V> changed = Collections.emptyMap();
                    boolean snapshot = false;
                    // skip tree updates on viewport change
                    if (event.updateEvent != null) {
                        int size = sortedValues.size();
                        snapshot = event.updateEvent.getType() == UpdateEvent.Type.SNAPSHOT;
                        changed = processUpdateEvent(event.updateEvent, filter, sortedValues, keys);
                        if (size != sortedValues.size() || snapshot) {
                            // force page recalculation on size change
                            page.set(new HashMap<>());
                        }
                    }

                    BlotterViewport viewport = event.viewport;
                    PageUpdate.Builder<V> builder = PageUpdate.<V>builder()
                            .totalSize(sortedValues.size())
                            .pageSize(viewport.getPageSize())
                            .pageNumber(viewport.getPageNumber());

                    if (viewport.isPaged()) {
                        handlePagedUpdate(viewport, page, builder, changed, sortedValues);
                    } else {
                        handleUnpagedUpdate(builder, snapshot, changed, sortedValues);
                    }
                    return builder.build();
                })
                .filter(update -> !update.isEmpty() || update.isSnapshot());
    }

    private void handlePagedUpdate(BlotterViewport viewport,
                                   AtomicReference<Map<K, V>> page,
                                   PageUpdate.Builder<V> builder,
                                   Map<K, V> changed,
                                   RedBlackBST<V, V> sortedValues) {

        if (viewport.getOffset() >= sortedValues.size()) {
            Map<K, V> old = page.getAndSet(Collections.emptyMap());
            builder.updated(Collections.emptyList())
                    .deleted(new ArrayList<>(old.values()));
            return;
        }

        long minRank = viewport.getOffset();
        long maxRank = minRank + viewport.getPageSize() - 1;

        V min = sortedValues.select((int) minRank);
        V max = maxRank >= sortedValues.size() ? sortedValues.max() : sortedValues.select((int) maxRank);
        Iterable<V> pageContent = sortedValues.keys(min, max);
        Map<K, V> current = new LinkedHashMap<>();
        for (V existing : pageContent) {
            current.put(existing.getRecordKey(), existing);
        }

        Map<K, V> old = page.getAndSet(current);
        if (old.isEmpty()) {
            builder.snapshot(true)
                    .updated(new ArrayList<>(current.values()))
                    .deleted(Collections.emptyList());
        } else {
            List<V> updated = current.values().stream()
                    .filter(value -> {
                        K key = value.getRecordKey();
                        return !old.containsKey(key) || changed.containsKey(key);
                    })
                    .collect(Collectors.toList());
            old.keySet().removeAll(current.keySet());
            if (updated.size() + old.size() < viewport.getPageSize()) {
                builder.updated(updated).deleted(new ArrayList<>(old.values()));
            } else {
                // report as snapshot change
                builder.updated(new ArrayList<>(current.values())).deleted(Collections.emptyList()).snapshot(true);
            }
        }
    }

    private void handleUnpagedUpdate(PageUpdate.Builder<V> builder,
                                     boolean snapshot,
                                     Map<K, V> updated,
                                     RedBlackBST<V, V> sortedTrades) {
        if (snapshot) {
            builder.snapshot(true)
                    .updated(toList(sortedTrades.keys()))
                    .deleted(Collections.emptyList());
        } else {
            Map<Boolean, List<V>> partitioned = updated.values().stream()
                    .collect(Collectors.partitioningBy(sortedTrades::contains));
            builder.updated(partitioned.get(true))
                    .deleted(partitioned.get(false));
        }
    }

    private static <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        for (T value : iterable) {
            list.add(value);
        }
        return list;
    }

    private Map<K, V> processUpdateEvent(UpdateEvent<K, V> updateEvent,
                                         Predicate<V> filter,
                                         RedBlackBST<V, V> sortedValues,
                                         Set<K> keys)
    {
        Map<K, V> updated = new HashMap<>();
        boolean snapshot = updateEvent.getType() == UpdateEvent.Type.SNAPSHOT;
        if (snapshot) {
            sortedValues.clear();
            keys.clear();
        }
        log.info("Processing trade update: {} {}", snapshot, updateEvent.getUpdates().size());
        for (UpdateEventEntry<K, V> event : updateEvent.getUpdates()) {
            V oldValue = getOldValue(sortedValues, keys, event);
            if (getRecordVersion(oldValue) > event.getVersion()) {
                log.info("Skip stale update for key {} {} to {} ", oldValue.getRecordKey(), oldValue.getRecordVersion(), event.getVersion());
                continue;
            }

            boolean valueDeleted = oldValue != null && sortedValues.delete(event.getOldValue()) != null;
            keys.remove(event.getRecordKey());
            V value = event.getValue();
            boolean valueUpdated = value != null && filter.test(value);
            if (valueUpdated) {
                sortedValues.put(value, value);
                keys.add(value.getRecordKey());
            }
            if ((valueDeleted || valueUpdated) && !snapshot) {
                V changed = valueUpdated ? value : event.getOldValue();
                updated.put(changed.getRecordKey(), changed);
            }
        }
        log.info("Processed");
        return updated;
    }

    private V getOldValue(RedBlackBST<V, V> sortedValues, Set<K> keys, UpdateEventEntry<K, V> event) {
        V oldValue = null;
        if (keys.contains(event.getRecordKey())) {
            if (event.getOldValue() != null) {
                oldValue = sortedValues.get(event.getOldValue());
            }
            if (oldValue == null) {
                oldValue = sortedValues.get(event.getValue());
            }
            if (oldValue == null) {
               oldValue = StreamSupport.stream(sortedValues.keys().spliterator(), false)
                       .filter(value -> value.getRecordKey() == event.getRecordKey())
                       .findAny()
                       .orElse(null);
               log.info("Old value by iteration for {} is {}", event.getRecordKey(), oldValue);
            }
        }
        return oldValue;
    }

    @Data
    @AllArgsConstructor
    private static class ContentUpdateEvent<K, V extends BlotterReportRecord<K>> {
        private BlotterViewport viewport;
        private UpdateEvent<K, V> updateEvent;
    }
}

