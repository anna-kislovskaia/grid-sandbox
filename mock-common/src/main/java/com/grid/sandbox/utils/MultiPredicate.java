package com.grid.sandbox.utils;

import lombok.AllArgsConstructor;

import java.util.List;
import java.util.function.Predicate;

@AllArgsConstructor
public class MultiPredicate<V> implements Predicate<V> {
    private final List<Predicate<V>> predicates;

    @Override
    public boolean test(V v) {
        boolean result = true;
        for (int i = 0; i < predicates.size() && result; i++) {
            result = predicates.get(i).test(v);
        }
        return result;
    }
}
