# Kafka Senior Interview and Production Checklist

## One-minute explanation

Kafka is a distributed append-only event streaming platform. Topics are split into partitions, partitions are replicated across brokers, producers append records, and consumer groups process partitions independently. Kafka provides durable event history, replay and partition-scoped ordering.

## Senior questions

### Why partitions?
Horizontal distribution, parallel writes/reads, storage distribution and consumer parallelism.

### Why key a message?
To control partition placement and preserve ordering for related entities.

### Why no global ordering?
Global ordering limits parallelism. Most systems need per-order, per-account or per-customer ordering.

### Why duplicates?
Retries, crashes after side effects and before offset commit, ambiguous network outcomes, replay and publisher crashes.

### How do you handle duplicates?
Stable event/business IDs, unique constraints/inbox patterns and idempotent downstream APIs.

### What is replication factor 3?
Three replicas of every partition.

### What is ISR?
Replicas currently considered in sync.

### Why min.insync.replicas?
To constrain the number of in-sync replicas required for durability-oriented acknowledgement.

### What is acks=all?
The producer waits for the strongest configured acknowledgement condition.

### Kafka transaction versus DB transaction?
Kafka transactions coordinate Kafka records and Kafka offsets. They do not automatically form one transaction with an external DB/provider.

### DB + Kafka?
Use transactional outbox plus idempotent consumption.

### Poison message?
A message that repeatedly fails. Bound retries and quarantine instead of retrying forever.

### Debug consumer lag?
Find group/topic/partition, determine whether lag grows, compare producer rate with processing capacity, inspect hot partitions/rebalances/downstream dependencies.

### Scale to 1M events/sec?
Start with records/sec and bytes/sec, choose partitions from measured throughput, distribute leaders, size disk/network, use batching/compression, provision consumer capacity and load-test realistically.

## Production checklist

### Topic
- owner defined;
- ordering boundary;
- partition key;
- partition count;
- replication;
- retention;
- compaction decision;
- event size limit;
- schema compatibility.

### Producer
- acks;
- idempotence;
- retries/timeouts;
- batching/compression;
- error metrics;
- trace context.

### Consumer
- group owner;
- offset strategy;
- idempotency;
- retry/DLQ;
- lag alerts;
- graceful shutdown;
- downstream protection.

### Security
- TLS where required;
- authentication;
- least privilege;
- secret management;
- network restrictions;
- audit logging.

### Operations
- dashboards;
- alerts;
- runbooks;
- capacity model;
- backup/DR;
- failure tests;
- upgrade plan.

## Final mental model

Think in five layers:

1. Log: partitions and offsets.
2. Distribution: brokers, leaders and replication.
3. Consumption: groups, assignments and commits.
4. Reliability: idempotence, transactions, retries and reconciliation.
5. Operations: security, observability, capacity and recovery.
