package com.grid.sandbox.model;

import com.hazelcast.core.EntryEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;

@Data
@AllArgsConstructor
@Getter
public class UpdateEvent<K, V> {
    public enum Type {
        SNAPSHOT, INCREMENTAL, INITIAL
    }

    private final Map<K, UpdateEventEntry<K, V>> updates;
    private final Type type;

    @Override
    public String toString() {
        if (type == Type.SNAPSHOT) {
            return "UpdateEvent{" + type + " size=" + updates.size() + "}";
        } else {
            StringJoiner joiner = new StringJoiner("\n");
            updates.values().forEach(update -> joiner.add(update.toString()));
            return "UpdateEvent{" + type + ":" + joiner.toString() + "}";
        }
    }

    public String toShortString() {
        return "UpdateEvent{" + type + ":" + updates.size() + "}";
    }

    public static UpdateEvent inital = new UpdateEvent(Collections.emptyMap(), Type.INITIAL);
}
