package com.example.txcache.app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Map;

public final class TopicAdmin {
    private TopicAdmin() {}

    public static void ensureTopics(AppProperties props) {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", props.bootstrapServers()))) {
            short rf = 1;
            admin.createTopics(List.of(
                    new NewTopic(props.inputTopic(), props.topicPartitions(), rf),
                    new NewTopic(props.cacheLogTopic(), props.topicPartitions(), rf),
                    new NewTopic(props.processedTopic(), props.topicPartitions(), rf)
            )).all().get();
        } catch (Exception ignored) {
        }
    }
}