# Apache Kafka — Production Engineering Reference

**Audience:** Senior Backend Engineers, Go Engineers, Distributed Systems Engineers and SRE/Platform Engineers.

**Purpose:** A practical reference covering Kafka architecture, internals, producers, consumers, partitions, replication, KRaft, delivery semantics, transactions, storage, retention, schemas, retries, outbox/inbox, Kafka Streams, Connect, security, performance, observability, Kubernetes, Go implementation and production design.

> Kafka evolves. Always validate exact defaults and supported features against the Kafka broker and client versions you operate. Modern Kafka uses KRaft for cluster metadata; ZooKeeper is historical/legacy architecture.

## Contents

1. Fundamentals
2. Architecture
3. Topics, partitions, records and offsets
4. Partitioning and ordering
5. Brokers and KRaft
6. Replication and ISR
7. Producer internals
8. Consumer internals
9. Consumer groups and rebalancing
10. Offsets and delivery semantics
11. Transactions and exactly-once
12. Storage internals
13. Retention and compaction
14. Schemas and event contracts
15. Retries and DLQ
16. Database consistency: outbox/inbox
17. Saga
18. Kafka APIs, Streams and Connect
19. Security
20. Performance and capacity planning
21. Monitoring and observability
22. Failure troubleshooting
23. Production configuration
24. Kafka with Go
25. Kubernetes
26. Order/payment reference architecture
27. Anti-patterns
28. Senior interview questions
29. Production checklist
30. Glossary


## Deep-Dive Handbook

The repository now contains focused production chapters so the material can be studied independently:

1. [Fundamentals and Architecture](docs/01-fundamentals-architecture.md)
2. [Partitions, Replication, ISR and Failover](docs/02-partitions-replication-failover.md)
3. [Producer Internals and Reliability](docs/03-producer-internals.md)
4. [Consumers, Groups and Rebalancing](docs/04-consumers-groups-rebalancing.md)
5. [Delivery Semantics and Transactions](docs/05-delivery-semantics-transactions.md)
6. [Storage, Retention and Compaction](docs/06-storage-retention-compaction.md)
7. [Schemas, Retries, DLQ, Outbox, Inbox and Saga](docs/07-schemas-retries-outbox-saga.md)
8. [Security, Observability and Performance](docs/08-security-observability-performance.md)
9. [Go, Kubernetes and Production Operations](docs/09-go-kubernetes-production.md)
10. [Order and Payment Reference Architecture](docs/10-payment-order-reference.md)
11. [Disaster Recovery and Multi-Cluster](docs/11-disaster-recovery.md)
12. [Senior Interview and Production Checklist](docs/12-interview-production-checklist.md)


## Advanced Deep-Dive Chapters

13. [Broker Request Flow and KRaft Internals](docs/13-broker-request-flow-and-kraft-internals.md)
14. [Producer Accumulator and Consumer Internals](docs/14-producer-accumulator-and-consumer-internals.md)
15. [Kafka with Go: Production Patterns](docs/15-go-production-patterns.md)
16. [High-Volume Kafka: 100K to 1M Events/sec](docs/16-high-volume-100k-1m-events.md)
17. [Production Configuration Reference](docs/17-production-config-reference.md)

These chapters complement the master reference below. Version-specific Kafka behavior must be checked against the broker/client version actually operated.

---

# 1. Fundamentals

## 1.1 What is Kafka?

Apache Kafka is a distributed, durable, partitioned event-streaming platform built around append-only logs.

The core model is:

    Producer -> Topic -> Partition -> Consumer

A topic is divided into partitions. Each partition is an ordered log. Each record has an offset within that partition. Partitions can be replicated across brokers.

Kafka is designed for:

- high throughput
- horizontal scaling
- durable event history
- partition-level ordering
- multiple independent consumers
- replay/reprocessing
- stream processing
- asynchronous service decoupling
- buffering and backpressure

## 1.2 When Kafka is appropriate

Kafka is a strong choice for:

- order/payment/inventory events
- audit streams
- telemetry
- log/event pipelines
- asynchronous microservices
- analytics pipelines
- stream processing
- event replay
- high-volume integrations

Do not automatically choose Kafka for every queue. A simple work queue may be better served by a cloud queue or another broker when replay, partitioning and long-lived event history are unnecessary.

## 1.3 Kafka versus a traditional queue

A traditional queue commonly focuses on delivering a message to a worker and acknowledging it.

Kafka stores an event log:

    Topic
      |
      +--> Consumer group A
      +--> Consumer group B
      +--> Consumer group C

Each consumer group tracks its own position. The same event can therefore be consumed independently by multiple logical subscribers and can be replayed while retained.

---

# 2. Architecture

A Kafka cluster contains brokers and a controller quorum.

    Kafka Cluster
    +-----------------------------+
    | Broker 1  Broker 2  Broker 3|
    |   P0        P1        P2    |
    |   P1        P2        P0    |
    +-----------------------------+
             ^       ^
             |       |
        Producers  Consumers

Important concepts:

### Producer
Application that writes records.

### Broker
Kafka server that stores partition replicas and serves client requests.

### Topic
Logical named stream.

### Partition
Ordered append-only log and primary unit of parallelism.

### Consumer
Application that reads records.

### Consumer group
Consumers cooperating to process a topic.

### Controller
Manages cluster metadata and coordination.

### Replica
Copy of a partition.

### Offset
Position of a record within a partition.

---

# 3. Topics, partitions, records and offsets

## 3.1 Topic

Examples:

    orders
    payment.events
    inventory.events

A topic is logical. Its physical data is spread across partitions and brokers.

## 3.2 Partition

Example:

    orders
      P0: 0 1 2 3 4
      P1: 0 1 2 3
      P2: 0 1 2 3 4 5

Each partition has its own offset sequence.

Offset 4 in P0 has no relationship to offset 4 in P1.

## 3.3 Record

A record can contain:

- key
- value
- timestamp
- headers
- topic
- partition
- offset

Example event:

    eventType = OrderCreated
    eventVersion = 1
    eventId = evt-123
    orderId = order-123
    customerId = customer-456
    occurredAt = 2026-10-07T10:00:00Z

Headers can carry correlation IDs, trace context, event version and other transport metadata.

## 3.4 Offset

Offsets identify positions in a partition.

    P0
    0 -> OrderCreated(101)
    1 -> OrderCreated(104)
    2 -> OrderCreated(108)

Kafka does not normally delete a record when a consumer processes it. Retention/compaction policies determine when data is removed.

---

# 4. Partitioning and ordering

## 4.1 Ordering guarantee

Kafka guarantees ordering within a partition, not across a multi-partition topic.

    P0: A -> B -> C
    P1: D -> E -> F

There is no global order across P0 and P1.

## 4.2 Key-based partitioning

If events for the same entity must remain ordered, use an appropriate key such as orderId.

    OrderCreated(123)       -> P2
    PaymentSucceeded(123)   -> P2
    OrderShipped(123)       -> P2

Different orders can use different partitions and execute in parallel.

This is one of the most important Kafka design patterns:

**Define the ordering boundary first, then choose the partition key.**

## 4.3 Global ordering

A single partition is the straightforward way to impose total ordering for a topic.

Trade-off:

    fewer partitions -> less parallelism
    more partitions  -> more parallelism, no global ordering

Most systems need per-customer, per-order or per-account ordering rather than global ordering.

## 4.4 Hot partitions

Bad partition keys create skew:

    P0 -> 90%
    P1 -> 3%
    P2 -> 4%
    P3 -> 3%

Adding consumers does not make one partition execute in parallel inside a traditional consumer group. Fix the key distribution or redesign the workload.

---

# 5. Brokers and KRaft

## 5.1 Broker

A broker:

- stores partition replicas
- handles produce requests
- handles fetch requests
- participates in replication
- exposes metrics
- participates in cluster operations

Clients first connect to bootstrap servers to obtain metadata, then communicate with the appropriate broker for each partition.

## 5.2 KRaft

KRaft is Kafka's Raft-based metadata quorum architecture.

Conceptually:

    Controller quorum
          |
          v
    Cluster metadata

    Brokers
          |
          v
    Partition data

Do not confuse controller metadata with application event storage.

KRaft processes can run broker and/or controller roles depending on deployment architecture. For larger production environments, separating controller and broker roles can provide operational benefits.

## 5.3 Failure domains

Distribute replicas across independent failure domains such as availability zones or racks.

A replication factor of three is common in production, but the correct value depends on durability, cost and availability requirements.

---

# 6. Replication, leaders, followers and ISR

## 6.1 Replication factor

Replication factor 3 means:

    Broker 1 -> P0 leader
    Broker 2 -> P0 follower
    Broker 3 -> P0 follower

There are three replicas of the partition.

## 6.2 Leader

A partition has a leader replica. Producer writes go to the partition leader.

## 6.3 Followers

Followers replicate the leader's log.

## 6.4 ISR

ISR means In-Sync Replicas.

Example:

    P0
    Leader = B1
    ISR = B1, B2, B3

If B3 falls too far behind:

    ISR = B1, B2

ISR is central to Kafka durability and leader-election behavior.

## 6.5 min.insync.replicas

This setting defines the minimum number of in-sync replicas required for writes that request the appropriate acknowledgement level.

A common durability-oriented design is:

    replication.factor = 3
    min.insync.replicas = 2
    producer acks = all

Treat these as design choices, not universal defaults.

## 6.6 Leader failure

If a leader fails, Kafka can elect an eligible replica.

Monitor:

- offline partitions
- under-replicated partitions
- ISR shrink/expand
- leader elections
- replica lag

## 6.7 Unclean leader election

Allowing an out-of-sync replica to become leader can improve availability but may lose records that were not replicated to that replica.

For critical systems, prefer durability unless the business explicitly accepts that trade-off.

---

# 7. Producer deep dive

Producer path:

    Application
       |
    serialize
       |
    choose partition
       |
    buffer/batch
       |
    network request
       |
    partition leader
       |
    replication
       |
    acknowledgement

## 7.1 Partition selection

The producer can use:

- explicit partition
- key-based partitioning
- load-balancing strategy
- custom partitioner

The key should reflect the business ordering boundary.

## 7.2 Batching

Kafka producers accumulate records into batches.

Benefits:

- fewer network requests
- better compression
- higher throughput
- better broker I/O efficiency

Trade-off:

- more buffering can add latency

Important producer controls include batch size, batching delay, buffer memory, compression and request limits.

## 7.3 Compression

Common codecs:

- gzip
- Snappy
- LZ4
- Zstandard

Compression reduces network and storage pressure but consumes CPU.

## 7.4 acks

acks=0:

Producer does not wait for broker acknowledgement. Lowest acknowledgement overhead and weakest durability assurance.

acks=1:

Leader acknowledges after accepting the record.

acks=all:

Leader waits according to the in-sync replica requirements.

For important business events, durability-oriented settings are normally preferred.

## 7.5 Retries

A network failure may leave the producer uncertain whether the broker accepted a record.

Retrying can cause duplicates.

## 7.6 Idempotent producer

Kafka's idempotent producer mechanism uses producer identity and sequence information to prevent duplicate appends caused by retry scenarios within its guarantees.

Important distinction:

**Idempotent Kafka publishing does not make a payment, database update or external API call globally idempotent.**

## 7.7 Producer tuning

Tune together:

- acks
- idempotence
- batching
- compression
- in-flight request behavior
- buffer memory
- request size
- delivery timeout
- partition count
- broker capacity

Measure throughput and latency before and after every important change.

---

# 8. Consumer deep dive

Consumer path:

    Broker
      |
    fetch
      |
    consumer buffer
      |
    deserialize
      |
    validate
      |
    business processing
      |
    commit offset

Consumers pull data and control their position.

This is why a consumer can rewind and replay data.

## 8.1 Processing pipeline

A production consumer often performs:

1. fetch
2. deserialize
3. validate
4. idempotency check
5. business operation
6. database/external side effect
7. metrics/tracing
8. offset commit

## 8.2 Backpressure

If:

    incoming rate > processing rate

lag increases.

Possible solutions:

- optimize processing
- batch work
- increase consumer parallelism
- increase partitions when necessary
- protect downstream dependencies
- reduce retry storms
- scale the actual bottleneck

Do not blindly increase consumers. More consumers can overload PostgreSQL, Redis or external APIs.

---

# 9. Consumer groups and rebalancing

Suppose:

    Topic = orders
    Partitions = P0 P1 P2 P3

Group:

    payment-service

Consumers:

    C1 -> P0
    C2 -> P1
    C3 -> P2
    C4 -> P3

Within a traditional consumer group, each partition is assigned to one consumer at a time.

## 9.1 Consumer count versus partitions

If:

    partitions = 3
    consumers = 5

then at most three consumers actively own partitions:

    C1 -> P0
    C2 -> P1
    C3 -> P2
    C4 -> idle
    C5 -> idle

Therefore partitions are a key scaling dimension.

## 9.2 Multiple consumer groups

Different groups consume independently:

    orders
      |
      +--> payment-group
      +--> inventory-group
      +--> analytics-group
      +--> notification-group

Each group has its own offsets.

## 9.3 Rebalancing

Rebalancing can happen when:

- consumer joins
- consumer leaves
- consumer crashes
- subscription changes
- group membership changes

A rebalance redistributes partitions.

Use modern assignment strategies and, where suitable, static membership/cooperative behavior to reduce unnecessary disruption.

---

# 10. Offsets and delivery semantics

## 10.1 Position and committed offset

A consumer can have fetched/processed records beyond the last committed offset.

Example:

    log end = 1000
    consumer position = 950
    committed = 940

After a crash, restart behavior uses the committed position according to the consumer's configuration.

## 10.2 At-most-once

Commit before processing:

    commit
      |
    process

Crash after commit but before processing can lose the event.

## 10.3 At-least-once

Process before commit:

    process
      |
    commit

Crash after processing but before commit can replay the event.

This is the most common reliable model.

Therefore:

**At-least-once processing requires idempotent business logic.**

## 10.4 Exactly-once

Kafka transactions can provide exactly-once processing semantics for Kafka input/output when used correctly.

This does not mean arbitrary external effects are exactly once.

For external systems use:

- idempotency keys
- unique constraints
- transactional state
- inbox/outbox
- provider-level idempotency
- reconciliation

## 10.5 Auto commit

Automatic commits are convenient but can obscure failure boundaries.

For critical business processing, explicit commit control often gives a clearer model.

---

# 11. Transactions and exactly-once

Kafka transactions allow a producer to atomically write records and commit consumer offsets as part of a transaction.

Conceptually:

    Consumer reads input
          |
    Transactional producer
          |
          +--> output topic A
          +--> output topic B
          +--> consumer offset
          |
        COMMIT

Important concepts:

- transactional producer identity
- transaction timeout
- producer fencing
- read_committed isolation
- atomic offset commit
- abort/commit behavior

## 11.1 Fencing

Kafka uses producer identity/epochs to prevent an older producer instance from continuing to write when a newer instance has taken ownership of the transactional identity.

This matters during failover and restarts.

## 11.2 Exactly-once boundary

Strong statement:

**Kafka can provide exactly-once semantics for Kafka-to-Kafka processing when transactions are correctly configured and used.**

Weak/incorrect statement:

**Kafka guarantees a payment happens exactly once.**

A payment provider is an external system. Its side effect needs its own idempotency/reconciliation model.

---

# 12. Storage internals

Kafka stores each partition as an append-only log.

A partition is divided into segments and associated indexes.

Conceptual structure:

    partition-0/
      log segment
      offset index
      time index
      next segment
      ...

Segmenting supports:

- retention
- deletion
- compaction
- indexing
- manageable file sizes

Kafka is designed around efficient append/log I/O and makes extensive use of the operating system filesystem/page cache.

The JVM heap is not the same thing as total memory required by a Kafka broker. Storage, page cache, network buffers and OS behavior matter.

---

# 13. Retention, compaction and tiered storage

## 13.1 Retention

Kafka can remove old data based on time and/or size policies.

Example:

    retain events for 7 days

Consumers can replay records only while the relevant records remain available.

## 13.2 Log compaction

Compaction keeps the latest state for keys, subject to Kafka's compaction semantics.

Example:

    key=123 value=CREATED
    key=123 value=PAID
    key=123 value=SHIPPED

A compacted topic is useful for:

- entity state
- changelogs
- caches
- configuration
- rebuilding materialized views

## 13.3 Tombstones

A null-valued record can act as a tombstone for a key in a compacted topic.

## 13.4 Tiered storage

Kafka supports tiered-storage capabilities that can move older log data to remote storage depending on Kafka version and configuration.

Use it as part of an explicit retention/capacity architecture. It is not a replacement for understanding backup and disaster recovery.

---

# 14. Schemas and event contracts

An event is a long-lived API contract.

A useful event envelope:

    eventId
    eventType
    eventVersion
    occurredAt
    producer
    correlationId
    causationId
    traceId
    payload

Common schema formats:

- JSON Schema
- Avro
- Protobuf

Choose based on compatibility, tooling and performance.

## 14.1 Compatibility

Design for:

- backward compatibility
- forward compatibility where required
- rolling deployments
- optional fields
- default values
- safe deprecation

Avoid silently changing the meaning or type of an existing field.

## 14.2 Event versioning

Options include:

    OrderCreated v1
    OrderCreated v2

or schema evolution with compatibility rules.

The goal is that old and new producers/consumers can coexist during deployment.

---

# 15. Retries and DLQ

Classify failures.

Transient:

- network timeout
- temporary database outage
- HTTP 503
- throttling

Permanent:

- invalid schema
- invalid business data
- unsupported event version

Retry transient failures with bounded exponential backoff.

A common conceptual flow:

    main topic
       |
    consumer
       |
     failure
       |
    retry topic(s)
       |
    final failure
       |
      DLQ

A DLQ should retain enough metadata to diagnose and replay the event:

- original topic
- partition
- offset
- event ID
- failure reason
- attempt count
- timestamp
- consumer/application version

Do not create infinite retry loops.

---

# 16. Database + Kafka consistency

The dual-write problem:

    DB commit succeeds
         |
    Kafka publish fails

or:

    Kafka publish succeeds
         |
    DB transaction fails

Now the systems disagree.

## 16.1 Outbox pattern

Use one DB transaction:

    BEGIN
      INSERT order
      INSERT outbox_event
    COMMIT

Then asynchronously publish:

    outbox -> Kafka

Example outbox fields:

    id
    aggregate_id
    event_type
    payload
    created_at
    published_at
    attempts
    status
    last_error

## 16.2 Duplicate outbox publication

A publisher may:

1. publish event
2. crash
3. fail to mark it published
4. publish it again

Therefore consumers still need idempotency.

Outbox solves DB/business-state + event creation atomicity. It does not eliminate all duplicates.

## 16.3 Inbox pattern

Consumer stores event IDs in an inbox table.

    BEGIN
      INSERT inbox(event_id)  -- UNIQUE
      UPDATE business_state
    COMMIT

If the same event is replayed, the unique constraint prevents a second business update.

---

# 17. Saga

A distributed business workflow often uses Saga instead of a distributed database transaction.

Example:

    OrderCreated
        |
    PaymentRequested
        |
    PaymentSucceeded
        |
    InventoryReserved
        |
    ShipmentCreated

Failure:

    InventoryFailed
        |
    RefundRequested
        |
    PaymentRefunded
        |
    OrderCancelled

## 17.1 Orchestration

A coordinator tells each service what to do.

## 17.2 Choreography

Services react to events and emit new events.

## 17.3 Idempotency

Every Saga step must tolerate retries.

Use:

- operation IDs
- event IDs
- unique constraints
- state-machine transitions
- provider idempotency keys

---

# 18. Kafka APIs, Streams and Connect

Kafka exposes core capabilities through:

### Admin API
Topics, partitions, configs, groups and cluster administration.

### Producer API
Publish records.

### Consumer API
Consume records.

### Kafka Streams
Application/library for stream transformations, joins, aggregations, windows and stateful processing.

### Kafka Connect
Standard framework for moving data between Kafka and external systems.

Examples:

    PostgreSQL -> Kafka
    Kafka -> Elasticsearch
    Kafka -> object storage
    Kafka -> warehouse

Source connector:

    external system -> Kafka

Sink connector:

    Kafka -> external system

Use Connect when a standard connector is more maintainable than custom integration code.

---

# 19. Security

Production Kafka should use layered security.

## 19.1 Encryption

Use TLS for relevant client, broker and controller communication.

## 19.2 Authentication

Common mechanisms include SASL and mutual TLS, depending on environment.

## 19.3 Authorization

Use ACLs or the appropriate authorization mechanism.

Example:

    payment-service:
      WRITE payment.events
      READ payment.commands

    analytics-service:
      READ payment.events

Avoid giving every service unrestricted access.

## 19.4 Secrets

Never hardcode passwords/certificates/API keys.

Use a secrets manager or appropriate Kubernetes secret integration.

## 19.5 Network security

Prefer private networking, restricted listeners, firewalls/security groups and network policies where applicable.

---

# 20. Performance and capacity planning

Kafka performance depends on the entire system.

Measure:

- records/sec
- bytes/sec
- producer latency
- consumer latency
- consumer lag
- broker request latency
- CPU
- memory
- disk throughput
- disk latency
- network throughput
- replication health

## 20.1 Partition sizing

Suppose testing shows one partition can safely handle 20 MB/s and the requirement is 200 MB/s.

Initial estimate:

    200 / 20 = 10 partitions

Then add headroom for spikes, uneven keys, consumer parallelism and growth.

Always benchmark with real payloads.

## 20.2 Consumer capacity

If one consumer processes 5,000 records/sec and input is 40,000 records/sec:

    40,000 / 5,000 = 8

You need roughly eight equivalent processing units, provided partitions and downstream capacity permit it.

## 20.3 Storage formula

Approximate logical daily storage:

    bytes/sec * 86,400

For 100 MB/s:

    100 * 86,400 = 8.64 TB/day

At replication factor 3:

    about 25.92 TB/day physical

This is simplified. Compression, indexes, retention behavior, segment overhead and tiered storage alter actual requirements.

## 20.4 Network

Replication produces additional network traffic beyond producer ingress. Plan network capacity for:

- producer traffic
- replica traffic
- consumer fetch traffic
- controller/metadata traffic
- operational overhead

---

# 21. Monitoring and observability

## 21.1 Broker

Monitor:

- broker availability
- controller health
- request latency
- request queueing
- CPU
- memory
- GC/runtime
- disk utilization
- disk latency
- network

## 21.2 Replication

Monitor:

- under-replicated partitions
- offline partitions
- ISR shrink/expand
- leader elections
- replica lag

## 21.3 Producer

Monitor:

- records/sec
- bytes/sec
- request latency
- retries
- errors
- batch size
- buffer pressure
- compression
- record queue time

## 21.4 Consumer

Monitor:

- consumer lag
- lag age
- records/sec
- processing latency
- commit failures
- rebalance count
- errors
- retry rate
- DLQ rate

## 21.5 Lag

Conceptually:

    lag = latest available offset - consumer position/committed position

Interpret lag over time.

A static lag of 100,000 may be harmless for a slow batch workload; rapidly increasing lag is usually more important.

## 21.6 Tracing

Propagate OpenTelemetry trace context through Kafka headers where appropriate:

    HTTP
      |
    Order Service
      |
    Kafka producer
      |
    Kafka consumer
      |
    Payment Service
      |
    DB

This enables end-to-end asynchronous tracing.

---

# 22. Failure troubleshooting

## 22.1 Consumer lag increasing

Investigate in this order:

1. producer ingress rate
2. consumer throughput
3. partition distribution
4. consumer CPU/memory/GC
5. DB latency
6. external API latency
7. retries
8. rebalances
9. broker latency
10. disk/network
11. downstream capacity

Then scale the actual bottleneck.

## 22.2 Hot partition

Inspect keys and partition distribution.

If one partition receives most traffic, consumers cannot compensate for the skew.

## 22.3 Under-replicated partitions

Check:

- broker failure
- disk saturation
- network saturation
- replica lag
- broker resource pressure

Sustained under-replication is a reliability issue.

## 22.4 Poison message

Check:

- schema
- payload
- validation
- application bug
- downstream failure

Use bounded retry and DLQ.

## 22.5 Duplicate business operation

Investigate:

- producer retry
- consumer replay
- commit failure
- rebalance
- application retry
- duplicate outbox publication

Then enforce idempotency at the business boundary.

## 22.6 Out-of-order events

Check:

- different partitions
- incorrect key
- multiple producers using different partitioning
- concurrent processing
- application-level retries

Kafka's ordering guarantee is partition-scoped.

## 22.7 Producer timeout

Check:

- leader availability
- ISR
- broker load
- disk latency
- network
- request size
- quotas
- metadata freshness
- request queues

---

# 23. Production configuration principles

Exact defaults change by Kafka version.

## Broker areas

- node identity
- KRaft/controller configuration
- listeners
- advertised listeners
- log directories
- replication defaults
- min in-sync replicas
- partition defaults
- request limits
- retention
- quotas
- security

## Producer areas

- bootstrap servers
- acks
- retries
- idempotence
- compression
- batch size
- batching delay
- buffer memory
- request size
- delivery timeout
- transactions

## Consumer areas

- bootstrap servers
- group ID
- auto offset reset
- auto commit
- fetch sizes
- polling/processing limits
- session/heartbeat settings
- assignment strategy
- isolation level

Never copy a production config blindly from a blog. Validate every setting against the Kafka broker version, client version, workload and failure model.

---

# 24. Kafka with Go

Common Go clients include:

- franz-go
- segmentio/kafka-go
- Confluent's Go client

Choose based on:

- required Kafka features
- transaction support
- performance
- client maturity
- observability
- operational familiarity

## 24.1 Producer

Use a long-lived producer rather than creating a producer per HTTP request.

Architecture:

    HTTP handler
       |
    service
       |
    event creation
       |
    serializer
       |
    Kafka producer
       |
    metrics/tracing

## 24.2 Consumer

    Kafka consumer
       |
    deserialize
       |
    validate
       |
    idempotency
       |
    business service
       |
    DB/external API
       |
    commit

## 24.3 Context

Use Go context for:

- cancellation
- deadlines
- graceful shutdown
- tracing

Do not allow message processing to run forever.

## 24.4 Graceful shutdown

Kubernetes commonly sends SIGTERM.

Recommended flow:

    SIGTERM
       |
    stop new fetch/work
       |
    finish bounded in-flight work
       |
    commit successful offsets
       |
    close consumer/producer
       |
    exit

## 24.5 Worker pools

Use bounded concurrency for expensive work.

Unbounded goroutines can overload downstream systems and make Kafka lag worse.

## 24.6 Idempotent Go consumer

A strong pattern:

    event
      |
    BEGIN DB transaction
      |
    INSERT event_id into inbox
      |
    duplicate? -> no-op
      |
    apply business update
      |
    COMMIT
      |
    commit Kafka offset

The database unique constraint becomes the final duplicate protection.

---

# 25. Kubernetes

Kafka clients commonly run in Kubernetes.

    Kubernetes
      |
      +--> Order Service
      +--> Payment Service
      +--> Inventory Service
                  |
                  v
              Kafka cluster

## 25.1 Scaling

Scale consumers using:

- consumer lag
- lag age
- processing rate
- CPU
- memory
- downstream capacity

Do not use CPU alone.

## 25.2 Partition constraint

For a traditional consumer group:

    12 partitions

means no more than 12 consumers can actively own those partitions at one time.

## 25.3 Readiness

A process being alive is not necessarily equivalent to being ready to process business traffic.

Define readiness based on actual application needs.

## 25.4 Termination

Use a sufficient termination grace period and graceful Kafka shutdown.

Avoid killing a consumer while it is halfway through a database transaction.

---

# 26. Production design: Order and Payment

Reference architecture:

    Client
      |
    API Gateway / Load Balancer
      |
    Order Service (Go)
      |
      +---- PostgreSQL
      |
      +---- Outbox
              |
              v
            Kafka
              |
       +------+--------+----------------+
       |               |                |
       v               v                v
    Payment         Inventory       Notification
    Service         Service          Service
       |               |
       v               v
    Payment DB      Inventory DB
       |
       v
    External Payment Provider

## 26.1 Topics

Possible:

    order.events
    payment.commands
    payment.events
    inventory.commands
    inventory.events
    notification.events

## 26.2 Keys

Possible:

    order.events -> orderId
    payment.events -> paymentId or orderId

Choose the key based on the ordering requirement.

## 26.3 Order creation

    BEGIN
      INSERT order
      INSERT outbox(OrderCreated)
    COMMIT

Then:

    outbox -> Kafka order.events

## 26.4 Payment processing

Payment consumer:

1. validate event
2. check idempotency
3. create payment attempt
4. call provider with provider idempotency key
5. record provider result
6. publish PaymentSucceeded or PaymentFailed
7. commit Kafka offset

## 26.5 Unknown payment result

A provider timeout does not always mean payment failed.

Model:

    SUCCESS
    FAILED
    PENDING / UNKNOWN

Then use provider webhooks/reconciliation as appropriate.

## 26.6 Reconciliation

Critical payment systems commonly need:

    internal payment records
          +
    Kafka events
          +
    provider records
          |
          v
    reconciliation
          |
          v
    mismatch detection
          |
          v
    repair workflow

Kafka is not automatically the authoritative financial ledger.

---

# 27. Anti-patterns

## One giant topic

A single generic topic containing unrelated domains makes ownership and schema governance difficult.

## One partition for everything

Easy ordering, poor parallelism.

## Too many partitions without a plan

Partitions have operational and resource costs.

## Treating Kafka as a relational database

Kafka is an event log. Use a database for arbitrary business queries and transactional state.

## Assuming exactly-once means external exactly-once

Kafka transactions do not make an external payment provider transactional with Kafka.

## No idempotency

At-least-once delivery/replay makes duplicate processing normal.

## Infinite retries

A poison message can block progress indefinitely.

## Ignoring lag

Lag is a primary operational signal.

## Scaling consumers without downstream protection

More consumers can overload PostgreSQL or external services.

## No schema governance

An event is an API. Breaking changes can break many services.

## Putting secrets into events

Kafka retains and replicates data. Do not place credentials or unnecessary sensitive information into topics.

---

# 28. Senior interview questions

### Fundamentals

1. What is Kafka?
2. Kafka versus RabbitMQ?
3. What is a topic?
4. What is a partition?
5. What is an offset?
6. What is a broker?
7. What is a consumer group?
8. Why does Kafka retain consumed messages?
9. How does replay work?

### Partitioning

10. How is a partition selected?
11. Why use a key?
12. How do you guarantee order for an orderId?
13. Can Kafka guarantee global ordering?
14. What is a hot partition?
15. How would you fix partition skew?

### Producer

16. Explain acks 0, 1 and all.
17. What is idempotent producer?
18. Why can retries create duplicates?
19. What is batching?
20. Why compress?
21. How would you tune producer throughput?

### Consumer

22. How does a consumer resume?
23. What is lag?
24. What happens when a consumer crashes?
25. What causes rebalancing?
26. Can 20 consumers efficiently process 10 partitions?
27. Explain at-most-once.
28. Explain at-least-once.
29. How do you make a consumer idempotent?

### Reliability

30. What is ISR?
31. What is replication factor?
32. What is min.insync.replicas?
33. What happens when a leader dies?
34. What is an under-replicated partition?
35. What is unclean leader election?

### Transactions

36. What is Kafka exactly-once?
37. Can Kafka guarantee a card is charged exactly once?
38. What is a Kafka transaction?
39. What is producer fencing?
40. What is read_committed?

### Architecture

41. Explain outbox.
42. Explain inbox.
43. Explain Saga.
44. How do you design retries/DLQ?
45. How do you design for 1M events/sec?
46. How do you troubleshoot 20M consumer lag?

## Strong lag-troubleshooting answer

    1. Check producer ingress.
    2. Check consumer processing rate.
    3. Check partition skew.
    4. Check consumer CPU/memory/GC.
    5. Check DB latency.
    6. Check external API latency.
    7. Check retry/DLQ rate.
    8. Check rebalances.
    9. Check broker latency.
    10. Check disk/network.
    11. Identify the actual bottleneck.
    12. Scale or optimize the bottleneck.
    13. Verify lag falls after the change.

Do not answer "add more consumers" without checking partitions and downstream capacity.

---

# 29. Production checklist

## Architecture

- [ ] Topic ownership defined
- [ ] Partition key documented
- [ ] Ordering boundary documented
- [ ] Partition count justified
- [ ] Replication factor justified
- [ ] Failure domains considered
- [ ] KRaft architecture documented
- [ ] Capacity model documented

## Producer

- [ ] acks chosen intentionally
- [ ] idempotence enabled where needed
- [ ] retry behavior understood
- [ ] compression selected
- [ ] batching tuned
- [ ] payload size controlled
- [ ] schema compatibility tested

## Consumer

- [ ] group ID defined
- [ ] offset strategy defined
- [ ] idempotency implemented
- [ ] retries bounded
- [ ] DLQ/recovery defined
- [ ] graceful shutdown implemented
- [ ] downstream limits protected
- [ ] lag monitored

## Consistency

- [ ] outbox used where required
- [ ] inbox/idempotency used where required
- [ ] unique event IDs
- [ ] external idempotency keys
- [ ] reconciliation for critical financial flows

## Operations

- [ ] broker monitoring
- [ ] producer monitoring
- [ ] consumer monitoring
- [ ] lag alerts
- [ ] under-replication alerts
- [ ] disk alerts
- [ ] network monitoring
- [ ] centralized logs
- [ ] tracing
- [ ] runbooks
- [ ] disaster recovery tested

## Security

- [ ] TLS
- [ ] authentication
- [ ] authorization
- [ ] least privilege
- [ ] secret management
- [ ] network restrictions
- [ ] sensitive-data review

---

# 30. Glossary

**Broker** — Kafka server that stores and serves partition data.

**Cluster** — Group of Kafka brokers/controllers.

**Consumer** — Client that reads records.

**Consumer group** — Logical subscriber whose members share partition assignments and offsets.

**Controller** — Component responsible for cluster metadata and coordination.

**ISR** — In-Sync Replicas.

**KRaft** — Kafka's Raft-based metadata quorum architecture.

**Key** — Record field used to influence partition placement and establish an ordering boundary.

**Leader** — Replica currently responsible for partition writes.

**Offset** — Position of a record inside a partition.

**Partition** — Ordered append-only log and unit of parallelism.

**Producer** — Client that publishes records.

**Record** — Kafka event/message.

**Replication factor** — Number of replicas for a partition.

**Retention** — Rules controlling how long records remain.

**Tombstone** — Null-valued record used in compacted topics to represent deletion of a key's state.

**Topic** — Named stream containing partitions.

**Consumer lag** — Difference between the latest available data position and a consumer's processing/committed position.

**Outbox** — Database table used to atomically persist business state and an event before asynchronous publication.

**Inbox** — Consumer-side persistence used to deduplicate event processing.

**DLQ** — Dead-letter topic for records that cannot be successfully processed.

---

# Final mental model

When designing any Kafka system, ask these questions in order:

    1. What is the event?
    2. Who owns the topic?
    3. What is the ordering boundary?
    4. What is the partition key?
    5. How many partitions are required?
    6. What replication factor is required?
    7. What failure domains exist?
    8. What delivery guarantee is required?
    9. How are offsets committed?
    10. How are duplicates handled?
    11. How are retries handled?
    12. What is the DLQ strategy?
    13. How does DB + Kafka consistency work?
    14. How does the schema evolve?
    15. What metrics prove the system is healthy?
    16. What happens during broker failure?
    17. What happens during consumer failure?
    18. What happens during downstream failure?
    19. How does the system scale?
    20. How is recovery tested?

The senior-level principle is:

**Kafka is not just a message queue. It is a distributed event log whose core scaling and reliability model comes from partitions, replication, offsets and consumer groups.**

A production design succeeds when those four mechanisms are combined with idempotency, schema governance, failure handling, observability and capacity planning.

## Official references

- Apache Kafka documentation: https://kafka.apache.org/documentation/
- Kafka design: https://kafka.apache.org/41/design/design/
- Kafka configuration: https://kafka.apache.org/43/configuration/
- Kafka broker configuration: https://kafka.apache.org/43/configuration/broker-configs/
