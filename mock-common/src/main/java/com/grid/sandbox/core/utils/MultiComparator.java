package com.grid.sandbox.core.utils;


import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MultiComparator<V> implements Comparator<V> {
    private final List<Comparator<V>> comparators;

    public MultiComparator(List<Comparator<V>> comparators) {
        this.comparators = comparators;
    }

    public MultiComparator(Comparator<V>... comparators) {
        this.comparators = Arrays.asList(comparators);
    }

    @Override
    public int compare(V o1, V o2) {
        int result = 0;
        for (int i = 0; i < comparators.size() && result == 0; i++) {
            result = comparators.get(i).compare(o1, o2);
        }
        return result;
    }
}
