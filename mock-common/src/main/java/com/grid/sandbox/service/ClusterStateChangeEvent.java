package com.grid.sandbox.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class ClusterStateChangeEvent {
    public enum Type {
        CONNECTED, MEMBER_ADDED, MEMBER_REMOVED, DISCONNECTED;
    }

    private Type type;

}
