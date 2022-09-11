package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.PropertyOptionsUpdateEntry;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertyOptionsTracker<V> {
    private final Map<String, Function<V, String>> mappers;
    private final AtomicReference<Map<String, PropertyOptionsUpdateEntry>> options = new AtomicReference<>(Collections.emptyMap());

    public PropertyOptionsTracker(Map<String, Function<V, String>> mapperFunctions) {
        this.mappers = mapperFunctions;
        options.set(generateOptions());
    }

    private Map<String, PropertyOptionsUpdateEntry> generateOptions() {
        return mappers.keySet().stream()
                .collect(Collectors.toMap(key -> key, PropertyOptionsUpdateEntry::new));
    }

    public boolean applyIncrementally(V value, V oldValue) {
        Map<String, PropertyOptionsUpdateEntry> currentOptions = options.get();
        for (String property : mappers.keySet()) {
            PropertyOptionsUpdateEntry options = currentOptions.get(property);
            Function<V, String> mapper = mappers.get(property);
            String propertyValue = mapper.apply(value);
            // property value changed
            if (oldValue != null && !Objects.equals(propertyValue, mapper.apply(oldValue))) {
                return false;
            }
            options.getOptions().add(propertyValue);
        }
        return true;
    }

    public Collection<PropertyOptionsUpdateEntry> getFilterOptions() {
        Map<String, PropertyOptionsUpdateEntry> currentOptions = options.get();
        return new ArrayList<>(currentOptions.values());
    }

    public void resetFilterOptions(Stream<V> values) {
        Map<String, PropertyOptionsUpdateEntry> updatedOptions = generateOptions();
        values.forEach(value ->
                mappers.forEach((key, mapper) -> updatedOptions.get(key).getOptions().add(mapper.apply(value))));
        options.set(updatedOptions);
    }

}
