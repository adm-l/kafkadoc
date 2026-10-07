# Producer Accumulator and Consumer Internals

## 1. Producer batching model

Conceptually, a producer maintains per-partition batches:

    application records
          |
          v
    partition selection
          |
    +-----+-----+
    |           |
   P0          P1
    |           |
  batch        batch
    |           |
    +-----+-----+
          |
      network I/O

This explains why partition distribution matters. If almost all records go to one partition, that partition becomes the bottleneck even when many brokers exist.

## 2. Batching trade-off

More batching can mean:

- fewer requests;
- better compression;
- higher throughput;
- more queueing latency.

Less batching can mean:

- lower latency;
- more requests;
- higher CPU/network overhead.

Kafka's producer configuration includes controls for batch size, buffering, compression and delivery timeout. Verify exact defaults for the Kafka/client version you deploy; current Apache configuration documentation is versioned. citeturn0search1

## 3. Delivery timeout

A producer's delivery timeout bounds the time used for queuing, acknowledgement and retriable send failures. It should be considered together with request timeout and batching delay, not tuned in isolation. citeturn0search0

## 4. In-flight requests

Multiple in-flight requests improve throughput but complicate retry ordering. Idempotent producer behavior is designed to maintain stronger ordering/deduplication guarantees within its supported constraints.

## 5. Consumer fetch loop

Conceptually:

    fetch records
       |
    deserialize
       |
    process
       |
    commit

A production application may separate polling/fetching from processing with bounded concurrency.

## 6. Bounded concurrency

Use:

    fetch -> bounded queue -> workers

not:

    fetch -> unlimited goroutines

The bounded queue protects memory and downstream systems.

## 7. Per-key ordering

If order matters:

    hash(key) -> worker shard

For example:

    worker = hash(orderID) % N

All events for the same order reach the same worker.

This is an application-level technique; the partition key should still be chosen so Kafka itself preserves the required ordering boundary.

## 8. Offset commit rule

For at-least-once processing:

    process successfully
         |
    commit offset

If process succeeds but commit fails, replay is expected. Idempotency is therefore part of the design.

## 9. Poison event

A worker should classify errors:

- retryable;
- permanent;
- dependency unavailable;
- malformed.

Only retry errors that can reasonably succeed later.
