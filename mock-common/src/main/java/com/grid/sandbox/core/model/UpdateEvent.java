package com.grid.sandbox.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.List;
import java.util.StringJoiner;

@Data
@AllArgsConstructor
@Getter
public class UpdateEvent<K, V extends BlotterReportRecord<K>> {
    public enum Type {
        SNAPSHOT, INCREMENTAL
    }

    private final List<UpdateEventEntry<K, V>> updates;
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

}
