package com.grid.sandbox.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.StringJoiner;

@Data
@AllArgsConstructor
@Getter
public class UpdateEvent<K, V extends BlotterReportRecord<K>> {
    public enum Type {
        SNAPSHOT, INCREMENTAL
    }

    private final Collection<UpdateEventEntry<K, V>> updates;
    private final Type type;

    @Override
    public String toString() {
        if (type == Type.SNAPSHOT) {
            return "UpdateEvent{" + type + " size=" + updates.size() + "}";
        } else {
            StringJoiner joiner = new StringJoiner("\n");
            updates.forEach(update -> joiner.add(update.toString()));
            return "UpdateEvent{" + type + ":" + joiner.toString() + "}";
        }
    }

    public boolean isSnapshot() {
        return type == Type.SNAPSHOT;
    }

    public boolean isEmpty() {
        return type == Type.INCREMENTAL && updates.isEmpty();
    }

    public String toShortString() {
        return "UpdateEvent{" + type + ":" + updates.size() + "}";
    }

    public static <K, V extends BlotterReportRecord<K>> UpdateEvent<K, V> empty() {
        return new UpdateEvent<K, V>(Collections.emptyList(), Type.INCREMENTAL);
    }
}
