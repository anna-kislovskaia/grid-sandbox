package com.grid.sandbox.model;

import lombok.AllArgsConstructor;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class FilterOptionBuilderImpl<V> implements FilterOptionBuilder<V> {
    private static final Comparator<FilterOptionUpdateEntry> nameComparator = Comparator.comparing(FilterOptionUpdateEntry::getName);
    private final Map<String, Function<V, String>> mappers;

    @Override
    public List<FilterOptionUpdateEntry> getFilterOptions(Stream<V> values) {
        Map<String, FilterOptionUpdateEntry> options = mappers.keySet().stream()
                .collect(Collectors.toMap(key -> key, FilterOptionUpdateEntry::new));
        values.forEach(value -> {
            mappers.forEach((key, mapper) -> options.get(key).getOptions().add(mapper.apply(value)));
        });
        return options.values().stream().sorted(nameComparator).collect(Collectors.toList());
    }

}
