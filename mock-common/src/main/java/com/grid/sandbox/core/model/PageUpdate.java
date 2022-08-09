package com.grid.sandbox.core.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
@Getter
public class PageUpdate<V> {
    @JsonProperty
    private List<V> updated;

    @JsonProperty
    private List<V> deleted;

    @JsonProperty
    private int totalSize;

    @JsonProperty
    private int pageNumber;

    @JsonProperty
    private int pageSize;

    @JsonProperty
    private boolean snapshot;

    @JsonProperty
    private List<FilterOptionUpdateEntry> filterOptions;

    @JsonProperty
    private int subscriptionId;

    public boolean isEmpty() {
        return updated.isEmpty() && deleted.isEmpty();
    }

}

