package com.grid.sandbox.utils;

import com.grid.sandbox.model.CallAccount;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import java.io.Serializable;

public class CallAccountRemoteFilterFactory implements Serializable, Factory<CacheEntryEventFilter<String, CallAccount>> {
    @Override
    public CacheEntryEventFilter<String, CallAccount> create() {
        return cacheEntryEvent -> true;
    }
}
