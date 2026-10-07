# Consumers, Consumer Groups and Rebalancing

## Consumer lifecycle

    join group
      -> assignment
      -> fetch
      -> process
      -> commit offset
      -> continue

Fetched position and committed group offset are different concepts.

## Traditional consumer groups

For four partitions:

    C1 -> P0
    C2 -> P1
    C3 -> P2
    C4 -> P3

If consumers exceed partitions, extra consumers are idle.

Maximum parallelism for a traditional group is bounded by partition count.

## Rebalancing

Common triggers:
- consumer joins;
- consumer leaves;
- crash;
- session timeout;
- subscription change;
- partition changes;
- coordinator changes.

Rebalances can temporarily increase latency and lag.

## Long processing

Do not block the consumer loop indefinitely. A bounded fetch/work architecture can separate polling from business processing, but ordering must be preserved when required.

For key ordering, route the same key to the same worker or process sequentially per partition.

## Commit semantics

At-most-once style:

    fetch -> commit -> process

A crash after commit can lose processing.

At-least-once style:

    fetch -> process -> commit

A crash after processing but before commit causes replay. This is usually preferred when paired with idempotency.

## Backpressure

If input rate exceeds processing rate, lag grows.

Fix the bottleneck:
- CPU;
- database;
- external API;
- serialization;
- locks;
- partition skew;
- retry storm.

Do not blindly add consumers because downstream systems can become overloaded.

## Lag diagnosis

Always inspect:
1. consumer group;
2. topic;
3. partition;
4. lag growth rate;
5. age of oldest unprocessed record;
6. producer rate;
7. processing latency;
8. downstream latency;
9. rebalances;
10. hot partitions.

## Share groups

Modern Kafka versions also provide share-group functionality. Share groups have different delivery and assignment semantics from traditional consumer groups. Treat their configuration and operational behavior separately and verify exact broker/client version support.

## Graceful shutdown

1. stop taking new work;
2. finish or safely cancel in-flight work;
3. commit only successful work;
4. leave group cleanly;
5. flush metrics;
6. close clients.

Never commit work that did not complete.
