package com.example.txcache.core;

public record EntityEnvelope<T, S, TS>(
        String productId,
        String entityId,
        EntityKind kind,
        T tValue,
        S sValue,
        TS tsValue,
        boolean tombstone
        ) {}