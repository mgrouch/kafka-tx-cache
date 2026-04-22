package com.example.txcache.core;

import java.time.Instant;

public record ProcessedEvent<T, S, TS>(
        String eventId,
        Instant processedAt,
        EntityEnvelope<T, S, TS> entity
        ) {}