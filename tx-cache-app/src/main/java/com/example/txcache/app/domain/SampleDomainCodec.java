package com.example.txcache.app.domain;

import com.example.txcache.core.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class SampleDomainCodec implements DomainCodec<TEntity, SEntity, TSEntity> {
    private final ObjectMapper mapper;

    private static final TypeReference<InputEvent<TEntity, SEntity, TSEntity>> INPUT =
            new TypeReference<>() {};
    private static final TypeReference<CacheLogRecord<TEntity, SEntity, TSEntity>> CACHE =
            new TypeReference<>() {};

    public SampleDomainCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public InputEvent<TEntity, SEntity, TSEntity> readInputEvent(String json) {
        try {
            return mapper.readValue(json, INPUT);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public CacheLogRecord<TEntity, SEntity, TSEntity> readCacheLogRecord(String json) {
        try {
            return mapper.readValue(json, CACHE);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String writeCacheLogRecord(CacheLogRecord<TEntity, SEntity, TSEntity> record) {
        try {
            return mapper.writeValueAsString(record);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String writeProcessedEvent(ProcessedEvent<TEntity, SEntity, TSEntity> event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}