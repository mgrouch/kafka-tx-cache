package com.example.txcache.core;

import java.time.Instant;

public record CacheMutation<T, S, TS>(
        String productId,
        String entityId,
        EntityKind kind,
        T tValue,
        S sValue,
        TS tsValue,
        boolean tombstone,
        Instant changedAt
        ) {}