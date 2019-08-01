package com.grid.sandbox.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;

@Data
@AllArgsConstructor
public class UpdateEvent {
    public enum Type {
        SNAPSHOT, INCREMENTAL, INITIAL
    }

    @Getter
    private final Map<String, CallAccountUpdate> updates;
    @Getter
    private final Type type;

    @Override
    public String toString() {
        return "UpdateEvent{[" +  updates.size() + "]" + type + "}";
    }

    public static UpdateEvent inital = new UpdateEvent(Collections.emptyMap(), Type.INITIAL);
}
