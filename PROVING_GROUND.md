# Trail proving ground

The proving ground exercises the real path: generator → JetStream → ingestor →
storage adapter → Explorer API → Explorer. It does not load a synthetic JSON
array into the browser.

## Dataset tiers

Use 100,000 events for ordinary development, 1,000,000 for local validation,
and 10,000,000 for the published ClickHouse proving run. A seed and logical
start time fully determine IDs, relationships, timestamps, failures, retries,
services, entities, executions, flows, and HTTP outcomes.

```bash
docker compose up --build
go run ./cmd/trail-generator --events 100000 --seed 42
go run ./cmd/trail-generator --events 1000000 --seed 42
go run ./cmd/trail-generator --events 10000000 --seed 42
```

Wipe the local reference data with `docker compose down -v`, then start it
again. This removes only the named Compose volumes.

The Compose file pins the last published MinIO community container for an
isolated developer proving environment. MinIO moved later community releases
to source-only distribution; production deployments should use a currently
maintained S3-compatible service or build and review the current server source.

## Required report metadata

Record the date, CPU, memory, OS, Go version, Docker version, database image,
generator flags, Trail commit, event batch size, publisher concurrency,
ingestor limits, and storage configuration. Do not compare runs that omit this
context.

Capture:

- generator events/sec, memory, emitted envelopes, bytes, and errors;
- ingestor events/sec, average database batch, memory, redeliveries, DLQ,
  write errors, and acknowledgement errors;
- JetStream messages, bytes, pending acknowledgements, and redeliveries;
- database row count, compressed disk size, and insert throughput;
- payload object count, logical bytes, and stored bytes;
- median, p95, and p99 latency for flow timeline, execution, entity attempts,
  recent failures, event-kind, and HTTP 5xx queries;
- Explorer API memory and first useful render for overview, list, and flow
  detail views.

Use cold and warm query samples and publish the raw samples with the summary.
The repository intentionally contains no hard-coded performance target.

## Measured 10M reference run — 2026-09-12

This is an observation, not a performance target. The host was an Apple M2
(8 logical CPUs, 16 GiB) running macOS 27.0, Go 1.26.2 darwin/arm64, and Docker
Engine 29.4.0 with a 3.9 GiB Docker memory limit. Other unrelated development
containers were active, so these are deliberately not presented as clean-room
database benchmark numbers. Trail's module compatibility target remained Go
1.24.1.

Configuration:

```text
NATS                 2.11, file-backed JetStream
ClickHouse           25.8
generator            --events 10000000 --seed 42 --batch-size 256
                     --publish-concurrency 32 --payload-every 10000
ingestor              3 processes, each max 128 messages / 8192 events
payload               local MinIO, gzip, S3-compatible adapter
```

Results:

| Measurement | Observed |
| --- | ---: |
| Generated events | 10,000,000 |
| Coherent stories | 625,000 |
| Flows / executions / entities | 2,187,500 / 625,000 / 208,334 |
| Failed flows / executions | 137,988 / 137,988 |
| HTTP 5xx events | 68,994 |
| Wire envelopes | 39,065 |
| Average batch size | 255.98 events |
| Generator wall time | 69.03 s |
| Generator rate | 144,865 events/s; 566 envelopes/s |
| JetStream stored bytes | 5,001,457,348 bytes |
| Generator maximum RSS (`time -l`) | 197,132,288 bytes |
| Durable ingestion window | 260.355 s |
| Durable database rate | 38,409 events/s |
| Ingestor post-drain RSS | 6.6–8.6 MiB per process |
| Dropped / publish / decode / write / ack errors | 0 / 0 / 0 / 0 / 0 |
| JetStream pending / redeliveries / DLQ | 0 / 0 / 0 |
| `trail_events` compressed size | 2.33 GiB |
| Summary-table compressed size | 896.57 MiB |
| Payload objects / MinIO data directory | 63 / 380 KiB |

Representative warm Explorer API requests were measured end-to-end with curl
while unrelated containers were active:

| Query | Observed |
| --- | ---: |
| Flow timeline | 1.277 s |
| Execution detail | 0.158 s |
| Entity attempts/flows | 0.386 s |
| Flow list, 50 rows | 1.904 s best observed after index build |
| Event kind, 50 rows | 1.350 s best observed |
| HTTP 5xx, 50 rows | 4.187 s best observed |

The list and HTTP timings expose clear next optimization work; they are not
hidden behind a fabricated target. Detail lookups use focused projections,
while list/aggregation queries still compete for a constrained local
ClickHouse instance. Browser bundle output was 230.17 KiB JavaScript (71.34 KiB
gzip) and 5.76 KiB CSS (1.82 KiB gzip). Browser paint timing was not recorded in
this run, so no first-render claim is made.

Before the measured run, a seed-42 1,000-event correctness smoke test produced
the expected 1,000 rows, 219 flows, 63 executions, 21 entities, 14 failed flows,
14 failed executions, and 7 HTTP 5xx events.

## PostgreSQL comparison

The reference run also loaded the identical seed-42 100,000-event dataset
through separate JetStream streams into each adapter. Both accepted 395
envelopes with no redeliveries or ingestion errors.

| Measurement | ClickHouse 25.8 | PostgreSQL 17 |
| --- | ---: | ---: |
| Durable ingestion window | 3.589 s | 12.713 s |
| Approximate ingestion rate | 27,863 events/s | 7,866 events/s |
| Event data / heap | 29.18 MiB | 56 MiB |
| Total event + index/summary footprint | 40.74 MiB | 99 MiB |
| Flow list (50) | 26.2 ms | 127.7 ms |
| Flow timeline | 14.8 ms | 1.8 ms |
| Execution detail | 6.8 ms | 1.1 ms |
| Entity attempts | 13.0 ms | 1.8 ms |
| HTTP 5xx list | 36.0 ms | 0.5 ms |
| Event-kind list | 34.0 ms | 0.4 ms |
| Failed execution list | 25.3 ms | 130.7 ms |

These are single warm end-to-end API observations, not statistically rigorous
latency distributions. They support a nuanced recommendation: ClickHouse
provided higher batch ingestion and a smaller total footprint in this run;
PostgreSQL was faster for targeted indexed lookups at 100k rows. ClickHouse is
still the reference choice for high volume and aggregation/retention, while
PostgreSQL is compelling at moderate volume and lower operational complexity.
