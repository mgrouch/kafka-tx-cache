package com.example.txcache.app.domain;

import com.example.txcache.core.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class SampleBatchProcessor implements BatchProcessor<TEntity, SEntity, TSEntity> {
    @Override
    public ProcessResult<TEntity, SEntity, TSEntity> process(
            ProductState<TEntity, SEntity, TSEntity> currentState,
            List<InputEvent<TEntity, SEntity, TSEntity>> batch
    ) {
        ProductState<TEntity, SEntity, TSEntity> state = currentState.copy();
        List<ProcessedEvent<TEntity, SEntity, TSEntity>> processed = new ArrayList<>();
        List<CacheMutation<TEntity, SEntity, TSEntity>> mutations = new ArrayList<>();

        for (InputEvent<TEntity, SEntity, TSEntity> event : batch) {
            EntityEnvelope<TEntity, SEntity, TSEntity> e = event.entity();

            CacheMutation<TEntity, SEntity, TSEntity> mutation = new CacheMutation<>(
                    e.productId(),
                    e.entityId(),
                    e.kind(),
                    e.tValue(),
                    e.sValue(),
                    e.tsValue(),
                    e.tombstone(),
                    Instant.now()
            );

            switch (e.kind()) {
                case T -> {
                    if (e.tombstone()) state.tById().remove(e.entityId());
                    else state.tById().put(e.entityId(), e.tValue());
                }
                case S -> {
                    if (e.tombstone()) state.sById().remove(e.entityId());
                    else state.sById().put(e.entityId(), e.sValue());
                }
                case TS -> {
                    if (e.tombstone()) state.tsById().remove(e.entityId());
                    else state.tsById().put(e.entityId(), e.tsValue());
                }
            }

            mutations.add(mutation);
            processed.add(new ProcessedEvent<>(event.eventId(), Instant.now(), e));
        }

        return new ProcessResult<>(state, processed, mutations);
    }
}