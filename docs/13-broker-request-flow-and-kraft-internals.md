# Kafka Broker Request Flow and KRaft Internals

## 1. End-to-end produce flow

    Go service
      |
      | Produce request
      v
    Kafka broker
      |
      +--> validate topic/partition
      +--> verify leadership
      +--> append to leader log
      +--> replicate to followers
      +--> satisfy acknowledgement condition
      v
    producer response

The client normally discovers metadata first. Bootstrap servers are used to discover the cluster; actual requests are routed using returned metadata. Apache's current broker configuration identifies KRaft essentials such as node.id, process.roles, controller.quorum.bootstrap.servers and controller.listener.names. citeturn0search6

## 2. Fetch flow

    Consumer
       |
       | Fetch(P0, offset=105)
       v
    Broker hosting P0
       |
       +--> locate data using log/index structures
       +--> return batches
       v
    Consumer

Consumers control their read position. Processing does not delete the record.

## 3. KRaft mental model

KRaft separates metadata consensus from partition data serving.

    Controller quorum
          |
          v
    Cluster metadata
          |
    +-----+------+
    |            |
  Broker       Broker
    |            |
 partition logs / replicas

The metadata quorum uses Raft-style consensus. Application records remain in partition logs.

## 4. Controller versus broker

A controller is responsible for cluster metadata operations such as partition leadership and related coordination. Brokers serve client data requests and store replicas.

Do not reason about KRaft as if controllers are a replacement for the partition logs. They coordinate metadata; they do not contain the business event stream.

## 5. Failure sequence

If a broker holding a partition leader fails:

1. failure is detected;
2. controller metadata is updated;
3. an eligible replica can become leader;
4. clients refresh metadata;
5. producers/consumers reconnect;
6. traffic resumes.

The duration and impact depend on detection, election, client metadata refresh and replica health.

## 6. Senior troubleshooting question

When an application reports Kafka timeout, separate:

- DNS/connectivity;
- authentication;
- metadata availability;
- partition leadership;
- broker request queue;
- disk latency;
- replication lag;
- network saturation;
- producer buffering;
- consumer processing.

A timeout is an observation, not a root cause.
