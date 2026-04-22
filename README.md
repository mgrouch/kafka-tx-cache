# kafka-tx-cache

Programmatic Spring Boot, no annotations.

## Semantics

- Topics:
    - `input-event`
    - `cache-log-event`
    - `processed-event`
- All topics are keyed by `productId`
- Processing is batched by:
    - `app.batch.max-records`
    - or `app.batch.max-wait-ms`
- One Kafka transaction per batch:
    - publish cache deltas
    - publish processed events
    - publish offset checkpoints
- On failure:
    - nothing is visible
    - nothing is checkpointed
- On restart:
    - replay owned cache-log partitions
    - rebuild Ehcache
    - restore latest input offsets for owned partitions
    - continue from exact committed cache+offset state

## Sharding

Startup args:

- `--app.shard-index=0`
- `--app.shard-count=3`

Ownership rule:

`topicPartition % shardCount == shardIndex`

This is static ownership.

## Build

```bash
mvn clean package