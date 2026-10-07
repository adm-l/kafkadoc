# High-Volume Kafka Design: 100K to 1M Events/sec

## 1. Start with bytes, not just records

Suppose:

    100,000 events/sec
    average event = 1 KB

Logical ingress:

    100,000 × 1 KB
    = 100 MB/sec
    ≈ 8.64 TB/day

Before replication, indexes, segment overhead and operational headroom.

For 1M events/sec:

    1,000,000 × 1 KB
    = 1 GB/sec
    ≈ 86.4 TB/day

This is a different infrastructure problem from 1M tiny events/sec.

## 2. Partition sizing

Do not say:

    1M events/sec = 1,000 partitions

without benchmarking.

Instead:

    measured partition throughput = X records/sec
    required partitions = peak records/sec / X

Then round up and add growth/failure headroom.

Also validate bytes/sec per partition.

## 3. Broker distribution

Spread partition leaders across brokers.

If one broker owns most hot leaders:

    Broker 1 = 500 MB/sec
    Broker 2 = 100 MB/sec
    Broker 3 = 100 MB/sec

the cluster is effectively constrained by Broker 1.

Rebalance partition leadership/replicas and improve key distribution.

## 4. Replication cost

With replication factor 3, a logical write is replicated. Physical network and storage traffic are therefore materially greater than logical ingress.

Capacity planning must include:
- client ingress;
- inter-broker replication;
- consumer egress;
- storage writes;
- storage reads;
- compression CPU.

## 5. Producer strategy

For high throughput:
- batch records;
- compress batches;
- use enough partitions;
- avoid tiny messages where possible;
- use asynchronous sends;
- monitor producer buffer pressure;
- benchmark p99 latency.

## 6. Consumer strategy

Calculate processing capacity.

Example:

    peak = 1,000,000 events/sec
    one consumer instance = 25,000 events/sec

Base requirement:

    1,000,000 / 25,000 = 40 instances

Then add:
- failure headroom;
- rebalance impact;
- downstream capacity;
- partition count;
- peak burst factor.

## 7. Database bottleneck

A Kafka cluster can absorb traffic faster than a database can process it.

Example:

    Kafka = 500k/sec
    consumer = 200k/sec
    DB = 50k writes/sec

The DB is the bottleneck.

Adding consumers from 10 to 100 can make the database failure worse.

Use:
- batching;
- bulk writes;
- partitioned tables where appropriate;
- caching;
- async workflows;
- sharding only when justified.

## 8. Hot-key problem

One account can generate a disproportionate amount of traffic.

If all events for that account must be strictly ordered, splitting them across partitions breaks that guarantee.

Options:
- accept per-subkey ordering;
- shard the entity with a sequencing strategy;
- serialize only the critical state transition;
- redesign the event model.

There is no free way to get unlimited parallelism and strict total ordering for one entity.

## 9. Backpressure

When consumers cannot keep up:

    producer rate
       >
    consumer capacity
       >
    downstream capacity

Choose intentionally whether to:
- slow producers;
- buffer in Kafka;
- increase consumers;
- batch downstream work;
- reject/defer noncritical work;
- shed load.

Kafka is a buffer, not an infinite storage system.

## 10. Recovery capacity

A system must also recover from backlog.

If:

    normal processing = 100k/sec
    peak backlog = 10 billion records

and recovery processing is also limited to 100k/sec, catch-up takes about:

    10,000,000,000 / 100,000
    = 100,000 sec
    ≈ 27.8 hours

Therefore recovery throughput is an architectural requirement.

## 11. Capacity worksheet

For each topic record:

    peak records/sec
    peak bytes/sec
    average event size
    max event size
    partitions
    replication factor
    retention
    compression
    producer count
    consumer groups
    consumer throughput
    recovery target

## 12. Load-test scenarios

Test:
1. normal peak;
2. 2x peak;
3. one broker unavailable;
4. one AZ unavailable;
5. one consumer group stopped;
6. downstream DB throttled;
7. producer retry storm;
8. hot partition;
9. large events;
10. backlog recovery.

The system should be judged by both steady-state throughput and failure recovery.
