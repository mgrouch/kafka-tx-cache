package com.example.txcache.app;

import org.springframework.core.env.Environment;

public record AppProperties(
        String bootstrapServers,
        String inputTopic,
        String cacheLogTopic,
        String processedTopic,
        int topicPartitions,
        int batchMaxRecords,
        long batchMaxWaitMs,
        long pollTimeoutMs,
        long cacheHeapEntries,
        String producerClientId,
        String transactionIdPrefix,
        int shardIndex,
        int shardCount
        ) {
public static AppProperties from(Environment env) {
        return new AppProperties(
        env.getRequiredProperty("app.bootstrap-servers"),
        env.getRequiredProperty("app.topic.input"),
        env.getRequiredProperty("app.topic.cache-log"),
        env.getRequiredProperty("app.topic.processed"),
        Integer.parseInt(env.getRequiredProperty("app.topic.partitions")),
        Integer.parseInt(env.getRequiredProperty("app.batch.max-records")),
        Long.parseLong(env.getRequiredProperty("app.batch.max-wait-ms")),
        Long.parseLong(env.getRequiredProperty("app.poll-timeout-ms")),
        Long.parseLong(env.getRequiredProperty("app.cache.heap-entries")),
        env.getRequiredProperty("app.producer.client-id"),
        env.getRequiredProperty("app.producer.transaction-id-prefix"),
        Integer.parseInt(env.getRequiredProperty("app.shard-index")),
        Integer.parseInt(env.getRequiredProperty("app.shard-count"))
        );
        }
        }