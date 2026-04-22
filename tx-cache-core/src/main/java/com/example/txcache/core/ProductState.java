package com.example.txcache.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ProductState<T, S, TS> implements Serializable {
    private final String productId;
    private final Map<String, T> tById;
    private final Map<String, S> sById;
    private final Map<String, TS> tsById;

    public ProductState(String productId) {
        this(productId, new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    public ProductState(String productId,
                        Map<String, T> tById,
                        Map<String, S> sById,
                        Map<String, TS> tsById) {
        this.productId = productId;
        this.tById = tById;
        this.sById = sById;
        this.tsById = tsById;
    }

    public String productId() { return productId; }
    public Map<String, T> tById() { return tById; }
    public Map<String, S> sById() { return sById; }
    public Map<String, TS> tsById() { return tsById; }

    public ProductState<T, S, TS> copy() {
        return new ProductState<>(
                productId,
                new LinkedHashMap<>(tById),
                new LinkedHashMap<>(sById),
                new LinkedHashMap<>(tsById)
        );
    }
}