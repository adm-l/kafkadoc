# Schemas, Retries, DLQ, Outbox, Inbox and Saga

## Event contract

A production event should have stable metadata such as:

    event_id
    event_type
    event_version
    aggregate_id
    occurred_at
    correlation_id
    payload

Do not silently change field meaning or reuse an old field name for new semantics.

## Schema evolution

Prefer additive changes. Define compatibility rules before multiple teams consume the event.

Common formats:
- JSON;
- Avro;
- Protobuf.

## Retry taxonomy

Retry transient failures:
- temporary network errors;
- temporary database outage;
- downstream overload.

Do not endlessly retry:
- invalid schema;
- malformed payload;
- permanent authorization failure;
- impossible business state.

## Retry topics

A common pattern:

    main topic
      -> retry-1m
      -> retry-5m
      -> retry-30m
      -> DLQ

Carry original topic, partition, offset, event ID, retry count and error metadata.

## Poison messages

A poison message repeatedly fails. Infinite retries can block progress and create storms. Quarantine it, alert, fix the consumer and replay deliberately.

## DLQ

A DLQ should support:
- error classification;
- searchable metadata;
- retention;
- alerting;
- ownership;
- controlled replay.

It is not a garbage dump.

## Outbox

Problem:

    DB COMMIT + Kafka PUBLISH

are not normally one atomic transaction.

Solution:

    DB transaction
      -> business row
      -> outbox row
             |
             v
       publisher -> Kafka

The business state and outbox event are committed together.

The publisher can still duplicate an event if it crashes after Kafka accepts the event but before marking the row published. Consumers therefore need idempotency.

## Inbox

A consumer can enforce event uniqueness inside its database transaction:

    BEGIN
      insert event_id with UNIQUE constraint
      apply business state
    COMMIT

Duplicate event IDs are ignored or treated as already completed.

## Saga

A saga coordinates a distributed business workflow:

    Order -> Payment -> Inventory -> Shipment

Failure uses compensation rather than distributed rollback.

Orchestration centralizes workflow decisions. Choreography lets services react to events. Choose based on workflow complexity and operational clarity.

Each step should define:
- state transition;
- timeout;
- retry;
- idempotency;
- compensation;
- reconciliation;
- terminal failure state.
