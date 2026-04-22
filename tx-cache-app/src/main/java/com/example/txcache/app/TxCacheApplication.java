package com.example.txcache.app;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

public final class TxCacheApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder()
                .sources(Empty.class)
                .web(WebApplicationType.NONE)
                .initializers(new TxCacheInitializer())
                .run(args);

        ctx.getBean(TransactionalWorker.class).runForever();
    }

    public static final class Empty {}
}