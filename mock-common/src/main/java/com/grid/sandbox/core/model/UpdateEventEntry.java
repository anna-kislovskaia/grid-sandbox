package com.grid.sandbox.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@Builder
@ToString
public class UpdateEventEntry<K, V> {
    private K key;
    private V value;
    private V oldValue;
}
