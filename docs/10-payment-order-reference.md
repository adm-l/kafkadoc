# Kafka Reference Architecture: Order and Payment

## Architecture

    Order API
       |
    Order DB
       |
    Outbox
       |
     Kafka
     / |  \
    /  |   \
Payment Inventory Shipping
   |
Provider
   |
Reconciliation

## Order creation

Inside one database transaction:

1. create order;
2. create OrderCreated outbox row;
3. commit.

The API returns success after the business transaction is durable.

## Outbox publishing

A publisher sends outbox events to Kafka. A crash can create duplicates, so downstream consumers use event IDs and idempotency.

## Payment consumer

The payment service receives the order event.

Persist:
- payment ID;
- order ID;
- amount;
- currency;
- status;
- idempotency key;
- provider transaction ID.

Then call the provider with a stable provider idempotency key.

## Hard failure

Provider accepts the charge, but the service times out.

Do not generate a new payment ID and charge again.

Instead:
- retry using the same provider idempotency key;
- query provider status when available;
- reconcile asynchronously.

## Payment state machine

    INITIATED
       |
    PROCESSING
      /   \
 FAILED   AUTHORIZED
             |
          CAPTURED
             |
           REFUND

Actual states depend on the provider and business model.

## Topics

Example boundaries:
- orders.events;
- payments.commands;
- payments.events;
- inventory.events;
- shipping.events;
- payment.retry;
- payment.dlq.

Choose topics based on ownership, retention, ordering, throughput and consumer semantics.

## Reconciliation

Compare provider records with internal records.

Classify:
- matched;
- pending;
- provider success/internal pending;
- internal success/provider missing;
- amount mismatch;
- duplicate;
- unknown.

Unknown states must create operational work rather than disappear.

## Refunds

Refunds should have their own state machine and idempotency key:

    REFUND_REQUESTED
      -> PROCESSING
      -> REFUNDED

or:

    REFUND_REQUESTED
      -> FAILED

## Failure matrix

| Failure | Response |
|---|---|
| Kafka publish timeout | retry/idempotent publishing |
| Consumer crash before commit | replay |
| DB commit succeeds, publish uncertain | outbox |
| Provider timeout | retry same idempotency key |
| Provider success, app crashes | query/reconcile |
| Duplicate event | inbox/idempotent handler |
| Permanent invalid event | DLQ |
| Payment pending too long | reconciliation |

## Core rule

A reliable payment system makes failures detectable, retryable, idempotent, auditable and reconcilable. Kafka is one component of that system, not the entire guarantee.
