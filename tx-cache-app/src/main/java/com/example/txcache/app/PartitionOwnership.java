package com.example.txcache.app;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

public final class PartitionOwnership {
    private PartitionOwnership() {}

    public static List<TopicPartition> owned(String topic, int partitions, int shardIndex, int shardCount) {
        List<TopicPartition> out = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            if ((p % shardCount) == shardIndex) {
                out.add(new TopicPartition(topic, p));
            }
        }
        return out;
    }
}