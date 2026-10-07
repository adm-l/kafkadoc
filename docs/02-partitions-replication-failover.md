# Partitions, Replication, ISR and Failover

## Partitions

Each partition is an ordered log:

    P0: 0 -> 1 -> 2 -> 3
    P1: 0 -> 1 -> 2 -> 3

There is no ordering relationship across partitions.

Partition count determines parallelism for traditional consumer groups and affects metadata, file count, recovery and rebalancing overhead.

## Partition keys

For order processing, order_id is a typical key when all events for one order must remain ordered.

    OrderCreated(100)      -> P2
    PaymentAuthorized(100) -> P2
    OrderShipped(100)      -> P2

Avoid low-cardinality keys that concentrate traffic.

## Hot partitions

Symptoms:
- one partition has much higher traffic;
- one partition has isolated consumer lag;
- more consumers do not improve the hot partition.

Solutions:
- improve key cardinality;
- distribute tenants/accounts;
- shard a hot entity when ordering permits;
- redesign the event flow;
- increase partitions only when keys can actually distribute traffic.

## Replication

Replication factor 3 means three replicas of every partition.

    P0
    B1 = leader
    B2 = follower
    B3 = follower

The replication unit is the partition.

## ISR

ISR means In-Sync Replicas.

    Leader B1
    ISR = B1,B2,B3

If B3 falls behind:

    ISR = B1,B2

Sustained ISR shrink is an operational signal.

## Durability pattern

A common durability-oriented design is:

    replication.factor = 3
    min.insync.replicas = 2
    producer acks = all

This is a design choice, not a universal default.

## Leader failure

When a leader fails, Kafka can elect an eligible replica. Monitor:
- offline partitions;
- under-replicated partitions;
- ISR changes;
- leader election rate;
- replica lag.

## Unclean election

An out-of-sync replica can preserve availability if allowed to become leader, but records missing from that replica can be lost. Critical workloads normally prefer durability.

## Failure domains

Distribute replicas across independent racks/AZs where possible. Do not put all replicas on one failure domain.

## Storage planning

Approximate retained physical storage from:

    records/sec × average bytes/record × retention seconds × replication factor

Then add compression effects, indexes, segment overhead, bursts and recovery headroom.
