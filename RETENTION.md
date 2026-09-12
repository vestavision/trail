# Trail retention

Retention is an optional, one-shot lifecycle operation. It is not initialized
by `trail.Init`, it does not run a scheduler, and it adds no work to Trail's
logging hot path. Run it from cron, a Kubernetes CronJob, Nomad, or manually.

No implicit retention period exists. The command refuses to run without an
event policy, event class, or explicit default event TTL. Retention also cannot
be constructed or run without an archive store. Trail writes and verifies a
versioned gzip-NDJSON bundle, commits its manifest to the event store catalog,
and only then permits deletion. There is no delete-without-archive mode.

## Configuration

Durations use Go duration syntax. Policies are checked in order and the first
match wins. Empty match properties are wildcards.

```json
{
  "event_policies": [
    {"name":"http-success", "match":{"kind":"provider.http", "http_success":true}, "ttl":"168h"},
    {"name":"http-error", "match":{"kind":"provider.http", "http_success":false}, "ttl":"720h"},
    {"name":"business", "match":{}, "ttl":"2160h"}
  ],
  "event_classes": {"audit":"8760h"},
  "payload_classes": {"http":"168h", "error":"720h", "audit":"8760h"},
  "default_payload_ttl":"168h",
  "event_batch_size":1000,
  "payload_batch_size":100,
  "payload_concurrency":1,
  "max_events":100000,
  "max_payloads":10000,
  "max_runtime":"10m",
  "payload_grace_period":"24h"
}
```

An event class can be attached with `convention.RetentionClass("audit")`.
Payload classes and absolute expiry already travel in `payload.Ref` as
`RetentionClass` and `RetainUntil`.

Event precedence is event class, first matching policy, default TTL, then keep
forever. Payload precedence is absolute `RetainUntil`, known payload class,
first matching payload policy, default payload TTL, then keep forever. An
unknown non-empty class is retained, rather than silently falling through.

## Running

The database and payload environment variables are the same as the ingestor
and Explorer API:

```bash
TRAIL_STORE=postgres \
TRAIL_POSTGRES_URL='postgres://trail:trail@localhost/trail?sslmode=disable' \
TRAIL_ARCHIVE_STORE=filesystem \
TRAIL_ARCHIVE_FILESYSTEM_ROOT='./trail-archives' \
trail-retention run --config retention.json --dry-run

TRAIL_STORE=clickhouse \
TRAIL_CLICKHOUSE_ADDR=localhost:9000 \
TRAIL_ARCHIVE_STORE=s3 \
TRAIL_ARCHIVE_S3_ENDPOINT=localhost:9000 \
TRAIL_ARCHIVE_S3_BUCKET=trail-archives \
trail-retention run --config retention.json
```

`--max-events`, `--max-payloads`, and `--max-runtime` can lower the configured
work budget for a manual or exploratory run.

Dry run evaluates the same bounded pages but does not stage payload candidates,
delete events, or delete objects. The command prints a JSON run summary and
returns a non-zero status on an operational failure.

## Archive and restore

Each bounded deletion batch produces an immutable `trail.archive/v1`
manifest, a gzip-compressed NDJSON event object, and copies of every referenced
payload object. The manifest is written last and is the commit marker. A failed
event write, payload copy, verification, or catalog commit leaves live events
untouched.

The archive destination is mandatory and should be independent from active
payload storage. It may use the same S3-compatible service with a separate
bucket or non-overlapping prefix. Archive objects have no Trail-managed expiry
in this version.

```bash
trail-retention archives --limit 100
trail-retention inspect --archive-id <sha256>
trail-retention restore --archive-id <sha256> --offset 0 --limit 1000
```

Restore copies and verifies payloads before merging events. PostgreSQL uses its
event ID primary key; ClickHouse uses its replacing key and an archive-specific
deduplication identity. Repeating a restore is safe. Restore large archives in
bounded chunks using `next_offset` until `complete` is true.

Explorer archive routes are available when archive storage is configured. The
`POST /api/v1/archives/{id}/restore` route additionally requires
`TRAIL_EXPLORER_ENABLE_ARCHIVE_RESTORE=true`. Trail does not own authentication;
deployments must protect restore through their existing application or reverse
proxy authorization.

## Database behavior

PostgreSQL records the verified archive mapping first, then stages payload
candidates and deletes each bounded event batch in a single transaction. Keep
autovacuum enabled and monitor dead tuples for large
retention workloads; Trail never runs `VACUUM` for you.

ClickHouse verifies the durable archive mapping before staging payload
candidates and issuing a synchronous bounded mutation. Physical disk space is
reclaimed by later merges, so a
successful run represents logical deletion rather than immediate disk
reclamation. Each mutation also rebuilds only the affected flow, execution,
and terminal summary keys so Explorer results remain correct without turning
every UI request into a raw-event aggregation. Trail does not use a table TTL
because dynamic ordered policies can differ between rows in the same
partition.

## Payload safety

Payload objects are content addressed and may be shared. Event deletion first
creates a durable candidate. A candidate must pass its expiry, wait through the
configured grace period, and have no remaining reference in live events before
the object is deleted. Failures remain retryable; a missing object is treated
as an idempotent success.

Filesystem deletion is confined to the configured root. S3-compatible deletion
is confined to the configured bucket and prefix. The retention engine only
uses references normalized from Trail events and never accepts Explorer input
or arbitrary object keys.

Payload GC is intentionally eventually consistent. A failed event deletion
cannot trigger payload deletion, while partial ClickHouse work can leave a
safe stale candidate or reference that delays reclamation.

The initial collector discovers orphans from references staged before event
deletion. Objects that were written successfully but never appeared in any
ingested Trail event are not enumerated automatically; retain them with the
payload backend's own conservative lifecycle policy until an inventory-based
collector is configured in a later release.

## Operational guidance

- Always run `--dry-run` after changing policies.
- Back up both archive storage and the database archive catalog; verified
  restore needs both.
- Start with small batch and work limits, then increase them while observing
  database latency, ClickHouse mutations, PostgreSQL dead tuples, and runtime.
- Run only one ClickHouse retention command at a time.
- Schedule externally; Trail deliberately has no loop or scheduler mode.
- Keep the grace period longer than the maximum expected ingestion/redelivery
  delay.
