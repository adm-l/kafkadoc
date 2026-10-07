# Producer Internals and Reliability

## Pipeline

    application
      -> serialization
      -> partition selection
      -> buffer
      -> batching
      -> compression
      -> produce request
      -> partition leader
      -> replication
      -> acknowledgement

## Batching

Batching reduces request overhead and often improves compression and throughput. Larger batches can add latency. Tune batch size, batching delay and buffer memory from measurements.

## acks

acks=0: no acknowledgement wait.

acks=1: leader acknowledgement.

acks=all: strongest normal acknowledgement condition; ISR requirements matter.

For critical business events, acks=all plus suitable min.insync.replicas is common.

## Retries

A timeout is ambiguous: the broker may have accepted the record while the response was lost. Retrying can therefore create duplicates.

## Idempotent producer

Kafka producer idempotence uses producer identity and sequence information to prevent duplicate appends caused by retry scenarios within Kafka's guarantees.

Important: Kafka producer idempotence does not make an external payment, database update or HTTP API call globally idempotent.

## Serialization

JSON is simple and readable. Avro is useful with schema-registry ecosystems. Protobuf provides strong typed contracts and efficient encoding.

Choose based on consumers, governance, tooling and compatibility needs.

## Producer metrics

Monitor:
- records/sec;
- bytes/sec;
- request latency;
- batch size;
- compression ratio;
- retries;
- errors;
- timeouts;
- buffer exhaustion;
- per-partition distribution.

## Production checklist

- durability chosen intentionally;
- idempotence enabled where appropriate;
- record size bounded;
- timeouts defined;
- retry behavior tested;
- stable partition keys;
- schema validation;
- trace/correlation metadata;
- broker/network failure tests.
