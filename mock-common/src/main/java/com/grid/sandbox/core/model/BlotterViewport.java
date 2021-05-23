package com.grid.sandbox.core.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class BlotterViewport {
    private int pageNumber;
    private int pageSize;
    public boolean isPaged() {
        return pageSize > 0;
    }
    public long getOffset() {
        return (long)pageNumber * (long)pageSize;
    }

    public static BlotterViewport UNPAGED = new BlotterViewport(0, 0);
}
