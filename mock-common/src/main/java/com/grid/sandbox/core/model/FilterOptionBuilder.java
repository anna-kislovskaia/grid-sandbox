package com.grid.sandbox.core.model;

import java.util.List;
import java.util.stream.Stream;

public interface FilterOptionBuilder<V> {
    List<FilterOptionUpdateEntry> getFilterOptions(Stream<V> values);
}
