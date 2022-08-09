package com.grid.sandbox.core.utils;


import java.util.Comparator;
import java.util.List;

public class MultiComparator<V> implements Comparator<V> {
    private final List<Comparator<V>> comparators;
    private final int size;

    public MultiComparator(List<Comparator<V>> comparators) {
        this.comparators = comparators;
        this.size = comparators.size();
    }

    @Override
    public int compare(V o1, V o2) {
        int result = 0;
        for (int i = 0; i < size && result == 0; i++) {
            result = comparators.get(i).compare(o1, o2);
        }
        return result;
    }
}
