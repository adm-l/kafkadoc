# Storage, Retention, Compaction and Tiered Storage

## Partition log

Kafka stores each partition as an append-only log split into segments. Segments make retention, indexing, recovery and compaction manageable.

## Indexes

Kafka maintains indexes that help locate records efficiently rather than scanning the entire log.

## Retention

Time-based and size-based retention determine when old records become eligible for deletion.

Retention is independent of consumer acknowledgement. A consumed event can remain stored; an unread event can expire.

## Compaction

Compaction keeps the latest value for a key, subject to compaction semantics.

Example:

    order-1 -> CREATED
    order-1 -> PAID
    order-1 -> SHIPPED

A compacted topic can eventually retain the latest state for order-1.

Useful for state topics and changelogs.

## Tombstones

A tombstone is a key with a null value used to represent deletion in compacted topics. Tombstones remain long enough for consumers to observe the deletion before compaction removes the key.

## Retention versus compaction

Delete retention bounds historical data by time/size.

Compaction preserves latest state per key.

Both can be enabled when the workload requires latest-state reconstruction plus bounded history.

## Tiered storage

Tiered storage can move older log data to remote storage. Evaluate:
- local disk;
- remote cost;
- fetch latency;
- recovery;
- network;
- retention;
- operational tooling.

## Capacity formula

Daily logical data:

    records/sec × average bytes/record × 86400

Retained logical data:

    daily data × retention days

Physical planning additionally considers:
- replication;
- compression;
- indexes;
- segments;
- compaction;
- bursts;
- recovery headroom.

Never size disks only for average traffic.
