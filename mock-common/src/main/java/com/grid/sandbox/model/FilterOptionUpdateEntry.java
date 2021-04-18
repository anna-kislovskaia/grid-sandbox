package com.grid.sandbox.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@AllArgsConstructor
@Getter
public class FilterOptionUpdateEntry {
    @JsonProperty
    String name;
    @JsonProperty
    Set<String> options;

    public FilterOptionUpdateEntry(String name) {
        this.name = name;
        this.options = new HashSet<>();
    }
}
