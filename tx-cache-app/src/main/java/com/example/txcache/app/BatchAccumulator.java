package com.example.txcache.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class BatchAccumulator {
    private final int maxRecords;
    private final long maxWaitMs;
    private final long pollTimeoutMs;

    public BatchAccumulator(int maxRecords, long maxWaitMs, long pollTimeoutMs) {
        this.maxRecords = maxRecords;
        this.maxWaitMs = maxWaitMs;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public List<ConsumerRecord<String, String>> drain(KafkaConsumer<String, String> consumer) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + maxWaitMs;

        while (all.size() < maxRecords) {
            long remain = Math.max(1L, deadline - System.currentTimeMillis());
            ConsumerRecords<String, String> polled =
                    consumer.poll(Duration.ofMillis(Math.min(remain, pollTimeoutMs)));

            for (ConsumerRecord<String, String> r : polled) {
                all.add(r);
                if (all.size() >= maxRecords) {
                    break;
                }
            }
            if (!all.isEmpty() && System.currentTimeMillis() >= deadline) {
                break;
            }
            if (System.currentTimeMillis() >= deadline) {
                break;
            }
        }
        return all;
    }
}