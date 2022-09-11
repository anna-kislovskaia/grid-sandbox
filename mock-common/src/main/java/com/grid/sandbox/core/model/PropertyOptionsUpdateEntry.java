package com.grid.sandbox.core.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class PropertyOptionsUpdateEntry {
    @JsonProperty
    String name;
    @JsonProperty
    Set<String> options;

    public PropertyOptionsUpdateEntry(String name) {
        this.name = name;
        this.options = new ConcurrentSkipListSet<>();
    }

    @Override
    public String toString() {
        return "PropertyOptions:" + name + "[" + options.size() + "]";
    }
}
