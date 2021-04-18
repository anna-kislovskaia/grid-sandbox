package com.grid.sandbox.model;

import java.util.List;
import java.util.stream.Stream;

public interface FilterOptionBuilder<V> {
    List<FilterOptionUpdateEntry> getFilterOptions(Stream<V> values);
}
