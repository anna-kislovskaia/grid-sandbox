package com.grid.sandbox.core.utils;

import java.util.List;
import java.util.function.Predicate;

public class MultiPredicate<V> implements Predicate<V> {
    private final List<Predicate<V>> predicates;
    private final int size;

    public MultiPredicate(List<Predicate<V>> predicates) {
        this.predicates = predicates;
        this.size = predicates.size();
    }

    @Override
    public boolean test(V v) {
        boolean result = true;
        for (int i = 0; i < size && result; i++) {
            result = predicates.get(i).test(v);
        }
        return result;
    }
}
