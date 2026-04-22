package com.example.txcache.core;

public record CacheLogRecord<T, S, TS>(
        CacheMutation<T, S, TS> mutation,
        OffsetCheckpoint checkpoint
        ) {
public static <T, S, TS> CacheLogRecord<T, S, TS> mutation(CacheMutation<T, S, TS> mutation) {
        return new CacheLogRecord<>(mutation, null);
        }

public static <T, S, TS> CacheLogRecord<T, S, TS> checkpoint(OffsetCheckpoint checkpoint) {
        return new CacheLogRecord<>(null, checkpoint);
        }

public boolean isMutation() {
        return mutation != null;
        }

public boolean isCheckpoint() {
        return checkpoint != null;
        }
        }