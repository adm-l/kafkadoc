# Kafka with Go, Kubernetes and Production Operations

## Go architecture

Separate:
- configuration;
- Kafka client;
- serialization;
- business handler;
- retry policy;
- idempotency;
- repository;
- metrics;
- tracing;
- shutdown lifecycle.

Pin the Kafka client version and keep it current.

## Consumer worker pool

A common pattern:

    Kafka consumer
        |
    bounded work queue
        |
    worker pool

Parallel workers can violate ordering. If ordering matters per key, route the same key consistently or process sequentially per partition.

## Idempotent consumer

A strong database-backed pattern:

1. validate event;
2. begin DB transaction;
3. insert event_id into inbox with UNIQUE constraint;
4. if duplicate, treat it as already processed;
5. apply business state;
6. commit DB transaction;
7. commit Kafka offset.

The exact offset/transaction integration depends on the Kafka client.

## Graceful shutdown

On SIGTERM:

    stop new work
      -> finish/cancel bounded work
      -> commit completed work
      -> flush producer
      -> close clients
      -> exit

Use a bounded shutdown deadline.

## Kubernetes

Kafka consumers are stateless from the pod perspective only if their external state and group membership are correctly managed.

For application Deployments:
- define readiness;
- define liveness;
- set resource requests/limits from measurements;
- expose metrics;
- use graceful termination;
- protect downstream systems.

Kafka itself may be managed, operator-based, or self-managed. Production Kafka should have an explicit operational ownership model.

## Scaling

CPU-only HPA is often insufficient for consumers. Consumer lag can be a useful scaling signal, combined with CPU and downstream capacity.

Never scale a traditional consumer group beyond useful partition parallelism.

## Deployment tests

Test:
- rolling restart;
- consumer crash;
- broker loss;
- network timeout;
- duplicate processing;
- rebalance;
- DLQ replay;
- downstream outage;
- graceful shutdown.

## Runbook: growing lag

1. identify group;
2. identify topic/partition;
3. determine whether lag is growing;
4. compare producer and consumer rates;
5. inspect processing latency;
6. inspect DB/API dependencies;
7. check partition skew;
8. check rebalances;
9. scale only when partitions and downstream systems permit;
10. verify lag age recovers.

## Runbook: under-replication

1. check broker health;
2. check disk;
3. check network;
4. check replica fetch lag;
5. check failed failure domains;
6. restore capacity;
7. monitor ISR recovery.

## Runbook: poison message

1. identify event ID;
2. classify error;
3. stop infinite retry;
4. quarantine/DLQ;
5. fix consumer;
6. verify replay safety;
7. replay selectively;
8. verify business state.
