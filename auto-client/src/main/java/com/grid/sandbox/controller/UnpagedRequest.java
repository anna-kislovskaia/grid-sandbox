package com.grid.sandbox.controller;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public class UnpagedRequest implements Pageable {

    private final Sort sort;

    public UnpagedRequest(Sort sort) {
        this.sort = sort;
    }

    public UnpagedRequest() {
        this(Sort.unsorted());
    }

    @Override
    public boolean isPaged() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public int getPageSize() {
        return 0;
    }

    @Override
    public long getOffset() {
        return 0;
    }

    @Override
    public Sort getSort() {
        return sort;
    }

    @Override
    public Pageable next() {
        return this;
    }

    @Override
    public Pageable previousOrFirst() {
        return this;
    }

    @Override
    public Pageable first() {
        return this;
    }

    @Override
    public boolean hasPrevious() {
        return false;
    }

}
