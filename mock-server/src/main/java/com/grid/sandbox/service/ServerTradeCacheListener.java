package com.grid.sandbox.service;

import com.grid.sandbox.model.Trade;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.MapEvent;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ServerTradeCacheListener extends EntryAdapter<String, Trade> {

    public void onEntryEvent(EntryEvent<String, Trade> event) {
        log.info("Entry changed -> {}", event.getValue());
    }

    public void onMapEvent(MapEvent event) {
        log.info("Map changed -> {}", event.toString());
    }
}
