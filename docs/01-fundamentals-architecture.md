# Kafka Fundamentals and Architecture

## Mental model

Kafka is a distributed append-only event log. A topic is split into partitions; each partition is an ordered sequence of records. Brokers store partition replicas, producers append records, and consumers fetch them.

Core flow:

    Producer -> Topic -> Partition -> Consumer Group

Core objects:
- Cluster: Kafka servers operating together.
- Broker: stores replicas and serves requests.
- Controller quorum: KRaft metadata quorum.
- Topic: logical event stream.
- Partition: ordered log and unit of parallelism.
- Record: key, value, timestamp and headers.
- Offset: position inside one partition.
- Replica: copy of a partition.
- Leader: replica receiving normal writes.
- Follower: replica that copies the leader.
- ISR: in-sync replicas.
- Consumer group: consumers cooperating over partitions.

## Request flow

A producer connects to bootstrap servers, obtains metadata, selects a partition, and sends data to that partition's leader. A consumer obtains metadata and fetches from brokers hosting its assigned partitions.

Bootstrap servers are discovery points, not a permanent proxy for all traffic.

## KRaft

Modern Kafka uses KRaft for cluster metadata instead of ZooKeeper. KRaft uses a Raft-based metadata quorum.

Keep two planes separate conceptually:

    Metadata plane: topics, partitions, leaders, cluster configuration
    Data plane: application records in partition logs

Kafka processes can be broker-only, controller-only, or combined. Combined mode is useful for small deployments; production environments commonly isolate controllers from brokers.

## Why Kafka scales

1. Topics can have many partitions.
2. Partitions can be distributed across brokers.
3. Producers can write concurrently.
4. Consumer groups process partitions concurrently.
5. Append-oriented storage is efficient.
6. Batching and compression reduce per-record overhead.

The fundamental scaling unit is the partition, not the topic.

## Before creating a topic

Define:
- business ordering boundary;
- partition key;
- records/sec and bytes/sec;
- average and maximum event size;
- retention;
- replay requirement;
- consumer groups;
- durability target;
- duplicate/loss tolerance;
- schema policy;
- failure domains.

Key principle: define the business ordering boundary first, then choose the partition key and partition count.
