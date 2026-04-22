package com.example.txcache.core;

import java.util.List;

public interface BatchProcessor<T, S, TS> {
    ProcessResult<T, S, TS> process(ProductState<T, S, TS> currentState,
                                    List<InputEvent<T, S, TS>> batch);
}