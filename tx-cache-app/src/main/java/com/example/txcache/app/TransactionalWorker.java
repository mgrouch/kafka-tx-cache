package com.example.txcache.app;

import com.example.txcache.app.domain.SEntity;
import com.example.txcache.app.domain.TEntity;
import com.example.txcache.app.domain.TSEntity;
import com.example.txcache.core.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ehcache.Cache;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public final class TransactionalWorker {

    private final AppProperties props;
    private final Cache<String, ProductState<TEntity, SEntity, TSEntity>> cache;
    private final KafkaTemplate<String, String> template;
    private final TransactionTemplate txTemplate;
    private final DomainCodec<TEntity, SEntity, TSEntity> codec;
    private final BatchProcessor<TEntity, SEntity, TSEntity> processor;

    private final Map<TopicPartition, Long> restoredOffsets = new HashMap<>();

    public TransactionalWorker(
            AppProperties props,
            Cache<String, ProductState<TEntity, SEntity, TSEntity>> cache,
            KafkaTemplate<String, String> template,
            TransactionTemplate txTemplate,
            DomainCodec<TEntity, SEntity, TSEntity> codec,
            BatchProcessor<TEntity, SEntity, TSEntity> processor
    ) {
        this.props = props;
        this.cache = cache;
        this.template = template;
        this.txTemplate = txTemplate;
        this.codec = codec;
        this.processor = processor;
    }

    public void runForever() {
        TopicAdmin.ensureTopics(props);

        List<TopicPartition> inputOwned = PartitionOwnership.owned(
                props.inputTopic(), props.topicPartitions(), props.shardIndex(), props.shardCount());

        List<TopicPartition> cacheOwned = PartitionOwnership.owned(
                props.cacheLogTopic(), props.topicPartitions(), props.shardIndex(), props.shardCount());

        restoreOwnedCache(cacheOwned);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(inputOwned);
            for (TopicPartition tp : inputOwned) {
                Long restored = restoredOffsets.get(tp);
                if (restored != null) {
                    consumer.seek(tp, restored);
                } else {
                    consumer.seekToBeginning(List.of(tp));
                }
            }

            BatchAccumulator accumulator = new BatchAccumulator(
                    props.batchMaxRecords(), props.batchMaxWaitMs(), props.pollTimeoutMs());

            while (true) {
                List<ConsumerRecord<String, String>> records = accumulator.drain(consumer);
                if (!records.isEmpty()) {
                    processBatch(records);
                }
            }
        }
    }

    private void restoreOwnedCache(List<TopicPartition> ownedCachePartitions) {
        try (KafkaConsumer<String, String> restoreConsumer = new KafkaConsumer<>(consumerProps())) {
            restoreConsumer.assign(ownedCachePartitions);
            restoreConsumer.seekToBeginning(ownedCachePartitions);
            Map<TopicPartition, Long> end = restoreConsumer.endOffsets(ownedCachePartitions);

            boolean done = false;
            while (!done) {
                var polled = restoreConsumer.poll(Duration.ofMillis(props.pollTimeoutMs()));
                for (ConsumerRecord<String, String> rec : polled) {
                    CacheLogRecord<TEntity, SEntity, TSEntity> log = codec.readCacheLogRecord(rec.value());
                    if (log.isMutation()) {
                        applyMutationToCache(log.mutation());
                    } else if (log.isCheckpoint()) {
                        OffsetCheckpoint cp = log.checkpoint();
                        restoredOffsets.put(new TopicPartition(cp.sourceTopic(), cp.sourcePartition()), cp.nextOffset());
                    }
                }
                done = true;
                for (TopicPartition tp : ownedCachePartitions) {
                    if (restoreConsumer.position(tp) < end.get(tp)) {
                        done = false;
                    }
                }
            }
        }
    }

    private void processBatch(List<ConsumerRecord<String, String>> batch) {
        Map<String, ProductState<TEntity, SEntity, TSEntity>> staged = new LinkedHashMap<>();
        List<ProcessedEvent<TEntity, SEntity, TSEntity>> processed = new ArrayList<>();
        List<CacheMutation<TEntity, SEntity, TSEntity>> mutations = new ArrayList<>();

        for (ConsumerRecord<String, String> rec : batch) {
            InputEvent<TEntity, SEntity, TSEntity> input = codec.readInputEvent(rec.value());
            String productId = input.entity().productId();

            ProductState<TEntity, SEntity, TSEntity> current =
                    staged.computeIfAbsent(productId, k -> {
                        ProductState<TEntity, SEntity, TSEntity> existing = cache.get(k);
                        return existing == null ? new ProductState<>(k) : existing.copy();
                    });

            ProcessResult<TEntity, SEntity, TSEntity> result = processor.process(current, List.of(input));
            staged.put(productId, result.newState());
            processed.addAll(result.processedEvents());
            mutations.addAll(result.cacheMutations());
        }

        Map<TopicPartition, Long> nextOffsets = new HashMap<>();
        for (ConsumerRecord<String, String> rec : batch) {
            TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
            nextOffsets.merge(tp, rec.offset() + 1, Math::max);
        }

        txTemplate.executeWithoutResult(status -> {
            for (CacheMutation<TEntity, SEntity, TSEntity> m : mutations) {
                template.send(new ProducerRecord<>(
                        props.cacheLogTopic(),
                        m.productId(),
                        codec.writeCacheLogRecord(CacheLogRecord.mutation(m))
                ));
            }

            for (Map.Entry<TopicPartition, Long> e : nextOffsets.entrySet()) {
                OffsetCheckpoint cp = new OffsetCheckpoint(e.getKey().topic(), e.getKey().partition(), e.getValue());
                String key = "checkpoint-" + e.getKey().partition();
                template.send(new ProducerRecord<>(
                        props.cacheLogTopic(),
                        key,
                        codec.writeCacheLogRecord(CacheLogRecord.checkpoint(cp))
                ));
            }

            for (ProcessedEvent<TEntity, SEntity, TSEntity> out : processed) {
                template.send(new ProducerRecord<>(
                        props.processedTopic(),
                        out.entity().productId(),
                        codec.writeProcessedEvent(out)
                ));
            }

            template.flush();
        });

        for (CacheMutation<TEntity, SEntity, TSEntity> m : mutations) {
            applyMutationToCache(m);
        }
        restoredOffsets.putAll(nextOffsets);
    }

    private void applyMutationToCache(CacheMutation<TEntity, SEntity, TSEntity> m) {
        ProductState<TEntity, SEntity, TSEntity> state = cache.get(m.productId());
        if (state == null) {
            state = new ProductState<>(m.productId());
        } else {
            state = state.copy();
        }

        switch (m.kind()) {
            case T -> {
                if (m.tombstone()) state.tById().remove(m.entityId());
                else state.tById().put(m.entityId(), m.tValue());
            }
            case S -> {
                if (m.tombstone()) state.sById().remove(m.entityId());
                else state.sById().put(m.entityId(), m.sValue());
            }
            case TS -> {
                if (m.tombstone()) state.tsById().remove(m.entityId());
                else state.tsById().put(m.entityId(), m.tsValue());
            }
        }
        cache.put(m.productId(), state);
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }
}