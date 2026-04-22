package com.example.txcache.core;

import java.time.Instant;

public record InputEvent<T, S, TS>(
        String eventId,
        Instant eventTime,
        EntityEnvelope<T, S, TS> entity
        ) {}