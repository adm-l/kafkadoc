# Production Kafka Configuration Reference

## Important rule

Kafka configuration is version-sensitive. The current Apache configuration index separates broker, topic, group, producer, consumer/share-consumer, Connect, Streams, Admin, MirrorMaker and tiered-storage settings. citeturn0search1

Do not blindly copy a configuration from a blog or an older Kafka release.

## Broker/KRaft essentials

Current Kafka broker documentation identifies these as essential KRaft configuration areas:

    node.id
    process.roles
    controller.quorum.bootstrap.servers
    controller.listener.names
    log.dirs

Verify exact listener, quorum and storage settings against the Kafka release you operate. citeturn0search6

## Durability-oriented topic

Conceptually:

    replication.factor = 3
    min.insync.replicas = 2

Producer:

    acks = all
    enable.idempotence = true

This combination is a common baseline for important business events, but availability and durability requirements must be evaluated together.

## Producer controls

Review:
- bootstrap.servers;
- acks;
- enable.idempotence;
- compression.type;
- batch.size;
- linger.ms;
- buffer.memory;
- delivery.timeout.ms;
- request.timeout.ms;
- max.in.flight.requests.per.connection;
- max.request.size.

The producer configuration documentation explicitly describes delivery timeout as the upper bound for queuing, acknowledgement and retriable send failures. citeturn0search0

## Consumer controls

Review:
- bootstrap.servers;
- group.id;
- enable.auto.commit;
- auto.offset.reset;
- fetch sizes;
- poll/fetch behavior;
- session/group settings;
- max partition fetch limits;
- isolation.level for transactional consumers.

Choose settings based on processing time and event size rather than arbitrary large values.

## Topic controls

Review:
- partitions;
- replication factor;
- retention.ms;
- retention.bytes;
- cleanup.policy;
- segment sizing;
- compression;
- max message size;
- min.insync.replicas.

## Security controls

Review:
- listener exposure;
- TLS;
- SASL mechanism;
- ACLs;
- certificate rotation;
- secret storage;
- network policy;
- audit trail.

## Upgrade discipline

Before upgrade:
1. identify broker/client versions;
2. read release notes;
3. verify compatibility;
4. test in staging;
5. test rolling restart;
6. verify consumer behavior;
7. verify transactions;
8. verify connectors/Streams;
9. monitor after rollout;
10. keep rollback/recovery procedure documented.

Kafka 4.3 was released May 22, 2026 and includes new features and deprecations; version-specific behavior should therefore be checked before changing production configuration. citeturn0search8
