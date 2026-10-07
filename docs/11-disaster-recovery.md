# Kafka Disaster Recovery and Multi-Cluster Design

## Define failure first

Consider separately:
- broker failure;
- availability-zone failure;
- region failure;
- cluster corruption;
- accidental deletion;
- credential compromise;
- producer outage;
- consumer outage;
- downstream outage.

## RPO and RTO

RPO is acceptable data loss.

RTO is acceptable recovery time.

Choose architecture from these requirements rather than starting with a replication tool.

## Multi-AZ

Distribute replicas across independent failure domains. Validate quorum, partition availability and capacity after losing one domain.

## Multi-cluster

Separate clusters can provide stronger isolation.

Patterns:
- active/passive;
- active/active;
- primary + DR;
- selected-topic replication.

MirrorMaker 2 and managed replication products are common options, depending on platform.

## Active/passive

Primary Kafka replicates to DR Kafka. This is easier operationally but DR resources may be underutilized.

## Active/active

Both regions process traffic. This requires explicit handling of:
- duplicate events;
- ordering;
- conflict resolution;
- global IDs;
- database consistency;
- ownership;
- failback.

Do not choose active/active only because it sounds more highly available.

## Backups

Protect:
- critical topic data;
- schemas;
- ACL/configuration;
- infrastructure definitions;
- required consumer/application state;
- DR procedures.

Replication is not a substitute for backups because bad data and accidental deletion can also replicate.

## DR test

1. declare simulated failure;
2. stop or isolate primary path;
3. route/promote DR;
4. validate producers;
5. validate consumers;
6. verify offsets/state;
7. verify database consistency;
8. replay missing events if needed;
9. measure RTO/RPO;
10. document gaps.

## Security incident

If credentials/certificates are compromised:
- rotate;
- revoke;
- review ACLs;
- identify affected topics;
- preserve evidence;
- validate integrity;
- rotate dependent application secrets.
