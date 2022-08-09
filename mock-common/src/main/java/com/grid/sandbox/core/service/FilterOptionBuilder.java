package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.FilterOptionUpdateEntry;

import java.util.List;
import java.util.stream.Stream;

public interface FilterOptionBuilder<V> {
    List<FilterOptionUpdateEntry> getFilterOptions(Stream<V> values);
}
