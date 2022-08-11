package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.PropertyOptionsUpdateEntry;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class PropertyOptionsTracker<V> {
    private final Map<String, Function<V, String>> mappers;
    private final AtomicReference<Map<String, PropertyOptionsUpdateEntry>> options = new AtomicReference<>(Collections.emptyMap());

    public boolean filtersChanged(V value) {
        Map<String, PropertyOptionsUpdateEntry> currentOptions = options.get();
        if (currentOptions.isEmpty()) {
            return true;
        }
        for (String property : mappers.keySet()) {
            PropertyOptionsUpdateEntry options = currentOptions.get(property);
            if (options == null || !options.getOptions().contains(mappers.get(property).apply(value))) {
                return true;
            }
        }
        return false;
    }

    public Collection<PropertyOptionsUpdateEntry> getFilterOptions() {
        Map<String, PropertyOptionsUpdateEntry> currentOptions = options.get();
        return currentOptions.values();
    }

    public void resetFilterOptions(Stream<V> values) {
        Map<String, PropertyOptionsUpdateEntry> updatedOptions = mappers.keySet().stream()
                .collect(Collectors.toMap(key -> key, PropertyOptionsUpdateEntry::new));
        values.forEach(value -> {
            mappers.forEach((key, mapper) -> updatedOptions.get(key).getOptions().add(mapper.apply(value)));
        });
        options.set(updatedOptions);
    }

}
