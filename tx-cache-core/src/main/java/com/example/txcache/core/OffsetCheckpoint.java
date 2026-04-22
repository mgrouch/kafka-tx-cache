package com.example.txcache.core;

public record OffsetCheckpoint(
        String sourceTopic,
        int sourcePartition,
        long nextOffset
        ) {}
