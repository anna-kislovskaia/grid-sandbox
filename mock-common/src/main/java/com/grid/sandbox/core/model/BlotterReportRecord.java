package com.grid.sandbox.core.model;

public interface BlotterReportRecord<K> {
    K getRecordKey();
    long getRecordVersion();
}
