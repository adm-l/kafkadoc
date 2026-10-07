# Kafka with Go: Production Patterns

## 1. Producer interface

Keep Kafka behind an application interface:

    type EventPublisher interface {
        Publish(ctx context.Context, topic string, key []byte, value []byte) error
    }

This keeps business logic independent of the Kafka library and simplifies testing.

## 2. Event envelope

Use a stable envelope:

    type Event struct {
        EventID       string
        EventType     string
        EventVersion  int
        AggregateID   string
        CorrelationID string
        OccurredAt    time.Time
        Payload       []byte
    }

The exact serialization can be JSON, Avro, Protobuf or another governed format.

## 3. Producer configuration

For a durability-oriented service, evaluate:

    acks=all
    enable.idempotence=true
    compression=zstd or lz4
    bounded delivery timeout
    appropriate batching

Do not copy these values blindly. Benchmark with real event sizes and failure tests.

## 4. Consumer handler

Recommended shape:

    func Handle(ctx context.Context, event Event) error

Keep Kafka-specific code at the boundary.

## 5. Idempotency table

Example PostgreSQL design:

    CREATE TABLE processed_events (
        consumer_name TEXT NOT NULL,
        event_id TEXT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (consumer_name, event_id)
    );

Business state and the processed-event insert should normally be in the same database transaction.

## 6. Transactional handler

Conceptually:

    BEGIN

    INSERT processed_events ...
    -- if duplicate: no-op

    UPDATE business_table ...

    COMMIT

Then commit the Kafka offset after successful business processing.

## 7. Graceful shutdown

Use:

    signal.NotifyContext(...)

Then:

1. stop accepting new application work;
2. stop polling or stop assigning new work;
3. drain a bounded worker queue;
4. commit successful work;
5. flush producer;
6. close connections.

Never wait forever during shutdown.

## 8. Concurrency

Avoid one goroutine per Kafka record without a bound.

Use a worker pool or partition/key sharding.

If strict partition ordering is required, process one partition sequentially or preserve partition sequence explicitly.

## 9. Observability

Include:

    topic
    partition
    offset
    event_id
    correlation_id
    trace_id

in structured logs/metrics where safe.

Do not log payment credentials, access tokens or other secrets.

## 10. Testing

Test:
- duplicate delivery;
- commit failure;
- consumer restart;
- broker unavailable;
- timeout after broker acceptance;
- malformed event;
- retry exhaustion;
- DLQ replay;
- DB deadlock;
- downstream timeout;
- graceful shutdown.

## 11. Benchmark

Measure:

    records/sec
    bytes/sec
    p50
    p95
    p99
    CPU
    memory
    GC
    network
    consumer lag

Do not call a Kafka implementation production-ready because it passes a functional test alone.
