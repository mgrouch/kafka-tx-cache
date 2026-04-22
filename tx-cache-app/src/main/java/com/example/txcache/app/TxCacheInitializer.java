package com.example.txcache.app;

import com.example.txcache.app.domain.*;
import com.example.txcache.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.*;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashMap;
import java.util.Map;

public final class TxCacheInitializer implements ApplicationContextInitializer<GenericApplicationContext> {
    @Override
    public void initialize(GenericApplicationContext ctx) {
        AppProperties props = AppProperties.from(ctx.getEnvironment());
        ctx.registerBean(AppProperties.class, () -> props);

        ctx.registerBean(ObjectMapper.class, () -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper;
        });

        ctx.registerBean(CacheManager.class, () ->
                CacheManagerBuilder.newCacheManagerBuilder()
                        .withCache("productStateCache",
                                CacheConfigurationBuilder.newCacheConfigurationBuilder(
                                        String.class,
                                        (Class<ProductState<TEntity, SEntity, TSEntity>>) (Class<?>) ProductState.class,
                                        ResourcePoolsBuilder.heap(props.cacheHeapEntries())))
                        .build(true));

        ctx.registerBean(Cache.class, () -> {
            CacheManager cm = ctx.getBean(CacheManager.class);
            return cm.getCache("productStateCache",
                    String.class,
                    (Class<ProductState<TEntity, SEntity, TSEntity>>) (Class<?>) ProductState.class);
        });

        ctx.registerBean(DomainCodec.class, () -> new SampleDomainCodec(ctx.getBean(ObjectMapper.class)));
        ctx.registerBean(BatchProcessor.class, SampleBatchProcessor::new);

        ctx.registerBean(DefaultKafkaProducerFactory.class, () -> {
            Map<String, Object> p = new HashMap<>();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
            p.put(ProducerConfig.CLIENT_ID_CONFIG, props.producerClientId());
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(p);
            pf.setTransactionIdPrefix(props.transactionIdPrefix() + props.shardIndex() + "-");
            return pf;
        });

        ctx.registerBean(KafkaTemplate.class, () ->
                new KafkaTemplate<String, String>(ctx.getBean(DefaultKafkaProducerFactory.class)));

        ctx.registerBean(KafkaTransactionManager.class, () ->
                new KafkaTransactionManager<String, String>(ctx.getBean(DefaultKafkaProducerFactory.class)));

        ctx.registerBean(TransactionTemplate.class, () ->
                new TransactionTemplate(ctx.getBean(KafkaTransactionManager.class)));

        ctx.registerBean(TransactionalWorker.class, () -> new TransactionalWorker(
                props,
                ctx.getBean(Cache.class),
                ctx.getBean(KafkaTemplate.class),
                ctx.getBean(TransactionTemplate.class),
                ctx.getBean(DomainCodec.class),
                ctx.getBean(BatchProcessor.class)
        ));
    }
}