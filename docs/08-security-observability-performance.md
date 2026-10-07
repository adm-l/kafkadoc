# Kafka Security, Observability and Performance

## Security

Use TLS for protected traffic, strong authentication, least-privilege authorization and private networking.

Protect:
- SASL credentials;
- private keys;
- certificates;
- cloud credentials.

Use a secrets manager or equivalent secret delivery system.

Do not expose brokers directly to the public internet without an explicit security architecture.

## Authorization

Restrict separately:
- topic read;
- topic write;
- consumer group access;
- administrative operations;
- transactional operations.

## Broker metrics

Monitor:
- CPU;
- memory;
- disk usage and latency;
- network;
- request latency;
- request queue time;
- offline partitions;
- under-replicated partitions;
- ISR changes;
- controller health.

## Producer metrics

Monitor:
- records/sec;
- bytes/sec;
- request latency;
- retries;
- errors;
- timeouts;
- batch size;
- compression;
- buffer pressure.

## Consumer metrics

Monitor:
- records/sec;
- bytes/sec;
- processing latency;
- commit latency;
- rebalance count;
- rebalance duration;
- consumer lag;
- lag age;
- retry rate;
- errors.

## Distributed tracing

Propagate trace context in Kafka headers:

    HTTP request
      -> trace context
      -> Kafka headers
      -> consumer
      -> DB/API calls

This allows asynchronous business operations to be traced end-to-end.

## Capacity planning

Start with:
- records/sec;
- average bytes/record;
- peak bytes/sec;
- retention;
- replication factor;
- consumer throughput;
- partition count.

Example:

    100,000 events/sec
    1 KB average event

Approximately 100 MB/sec logical ingress and 8.64 TB/day before compression. Replication increases physical storage/network implications.

Consumer capacity:

    required instances ≈ peak records/sec / records/sec per instance

Then add failure headroom and downstream capacity constraints.

## Partition planning

Partitions are a concurrency budget.

Too few:
- insufficient parallelism;
- hot partitions;
- slower recovery.

Too many:
- metadata overhead;
- more files;
- more recovery/rebalance work.

Benchmark with realistic event sizes and keys.

## Alerts

Alert on:
- offline partitions;
- sustained under-replication;
- disk threshold;
- controller/quorum problems;
- rapidly growing lag;
- repeated rebalances;
- producer errors;
- request latency;
- authentication failures.

The monitoring system must answer: what failed, where, since when, business impact, what changed, and safe recovery action.
