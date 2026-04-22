package com.example.txcache.core;

import java.util.List;

public record ProcessResult<T, S, TS>(
        ProductState<T, S, TS> newState,
        List<ProcessedEvent<T, S, TS>> processedEvents,
        List<CacheMutation<T, S, TS>> cacheMutations
        ) {}