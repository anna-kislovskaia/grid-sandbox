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
        if (this.value == null) {
            throw new IllegalArgumentException("Value must be provided");
        }
        if (this.oldValue != null) {
            if (!this.value.getRecordKey().equals(this.oldValue.getRecordKey())) {
                throw new IllegalArgumentException("Key records must be equal: " + this.toString());
            }
            if (this.value.getRecordVersion() < this.oldValue.getRecordVersion()) {
                throw new IllegalArgumentException("New version must be greater than old version: " + this.toString());
            }
        }
    }

    public long getVersion() {
        return value.getRecordVersion();
    }


    public static <K, V extends BlotterReportRecord<K>> UpdateEventEntry<K, V> addedValue(V value) {
        return new UpdateEventEntry<>(value, null);
    }

    public K getRecordKey() {
        return this.value != null ? this.value.getRecordKey() : this.oldValue.getRecordKey();
    }
}
