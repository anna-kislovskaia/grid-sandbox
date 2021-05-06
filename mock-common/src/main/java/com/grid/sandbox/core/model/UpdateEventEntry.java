package com.grid.sandbox.core.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class UpdateEventEntry<K, V extends BlotterReportRecord<K>> {
    private V value;
    private V oldValue;

    public UpdateEventEntry(V value, V oldValue) {
        this.value = value;
        this.oldValue = oldValue;
        if (this.value == null && this.oldValue == null) {
            throw new IllegalArgumentException("Either new or old value must be defined");
        }
        if (this.value != null && this.oldValue != null) {
            if (!this.value.getRecordKey().equals(this.oldValue.getRecordKey())) {
                throw new IllegalArgumentException("Key records must be equal: " + this.toString());
            }
            if (this.value.getRecordVersion() < this.oldValue.getRecordVersion()) {
                throw new IllegalArgumentException("New version must be greater than old version: " + this.toString());
            }
        }
    }

    public K getRecordKey() {
        return this.value != null ? this.value.getRecordKey() : this.oldValue.getRecordKey();
    }
}
