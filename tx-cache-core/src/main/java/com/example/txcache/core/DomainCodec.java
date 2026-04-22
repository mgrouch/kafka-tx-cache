package com.example.txcache.core;

public interface DomainCodec<T, S, TS> {
    InputEvent<T, S, TS> readInputEvent(String json);
    CacheLogRecord<T, S, TS> readCacheLogRecord(String json);
    String writeCacheLogRecord(CacheLogRecord<T, S, TS> record);
    String writeProcessedEvent(ProcessedEvent<T, S, TS> event);
}