# Delivery Semantics, Transactions and Exactly-Once

## At-most-once

    fetch -> commit -> process

A crash after commit can lose processing.

## At-least-once

    fetch -> process -> commit

A crash between processing and commit causes replay. Consumers must be idempotent.

## Exactly-once

Kafka supports transactional exactly-once processing for Kafka-to-Kafka workflows when producer, consumer, transaction and isolation settings are correctly designed.

It does not make an external payment provider exactly-once.

## Kafka transactions

A transactional producer can atomically publish multiple records and commit consumed offsets as part of a Kafka transaction.

Conceptually:

    consume input
    begin transaction
    produce outputs
    commit consumed offsets
    commit transaction

If the transaction aborts, read_committed consumers do not expose the aborted records.

## Transactional IDs

A stable transactional ID lets Kafka identify producer sessions and fence stale producers after restart/failover.

## read_committed

read_committed exposes committed transactional records and hides aborted records. Open transactions can hold back the last stable offset, so read_committed consumers can temporarily be behind the high watermark.

## DB + Kafka

Do not assume a database transaction and Kafka transaction form one distributed atomic transaction.

Use the transactional outbox when database state and an event must correspond.

## External payment

For:

    Kafka -> Payment Provider

use:
- stable payment/business ID;
- provider idempotency key;
- safe retry;
- status query when supported;
- reconciliation.

The business guarantee comes from the complete workflow, not Kafka EOS alone.
