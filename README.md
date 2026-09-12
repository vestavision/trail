# Trail

[![CI](https://github.com/vestavision/trail/actions/workflows/ci.yml/badge.svg)](https://github.com/vestavision/trail/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/vestavision/trail.svg)](https://pkg.go.dev/github.com/vestavision/trail)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Trail is a lightweight event, flow, and execution journal for Go applications.
It makes structured operational events nearly as easy to emit as `fmt.Println`,
while preserving enough identity to reconstruct a business flow, entity history,
or execution later in the sink of your choice.

## Live demo

Explore a deterministic one-million-event Trail dataset at
**[trail.vestavision.io](https://trail.vestavision.io)**. The hosted Explorer
demonstrates flow timelines, execution relationships, entity history, HTTP
events, structured fields, and payload references using synthetic demo data.

```go
trail.Init(trail.Config{
    Service:     "order-worker",
    Environment: "production",
    Sink:        sink,
})
defer trail.Close()

flowID := trail.NewFlow()

trail.Log(
    "order.fulfillment.started",
    trail.Flow(flowID),
    trail.Entity("order", orderID),
)

trail.Log(
    "order.fulfillment.inventory_reserved",
    trail.Flow(flowID),
    trail.Entity("order", orderID),
    trail.String("warehouse", "warehouse-a"),
    trail.Int("score", 97),
)
```

Trail is deliberately not an OpenTelemetry implementation, APM, Sentry
replacement, scheduler, distributed tracing framework, database, or query
service. It does not introduce traces or spans into its core model.

## Design

The package-level API is intentional. Operational journaling is cross-cutting,
and requiring every application function to accept a logger or `context.Context`
would make adoption more expensive than emitting the event. Initialize Trail
once, then call `trail.Log` wherever the business event occurs. Applications can
still put a `FlowID` or `ExecutionID` in a context when that is convenient; Trail
simply does not require it.

### Flows are identifiers, not buffers

`NewFlow` generates only a compact 128-bit value. Trail never keeps a
`flow ID -> events` map and never holds an entire flow in memory. Every call to
`Log` creates an independent event containing its `flow_id`, and the configured
sink publishes bounded batches. A downstream journal can reconstruct the flow
by selecting events with that ID.

An **entity** is a durable business object, such as `order/order_123`. A **flow**
is one path or attempt involving that object. One order may participate in many
fulfillment flows, and one flow may touch several entities.

An **execution** is a particular job run or manual operation. It can contain
many flows. Trail correlates the events produced by an execution but does not
schedule it or keep lifecycle state for it:

```text
execution: nightly catalog sync

├── flow A: order fulfillment attempt
│   ├── inventory loaded
│   ├── stock reserved
│   └── fulfillment confirmed
│
└── flow B: order fulfillment attempt
    ├── inventory loaded
    ├── stock unavailable
    └── fulfillment rejected
```

```go
executionID := trail.NewExecution()
flowID := trail.NewFlow()

trail.Log("catalog.sync.started", trail.Execution(executionID))
trail.Log("order.fulfillment.inventory_loaded",
    trail.Execution(executionID),
    trail.Flow(flowID),
    trail.Entity("order", orderID),
)
```

Retries and child executions are also explicit, stateless metadata. A retry is
a new execution rather than an update to an old one:

```go
retryID := trail.NewExecution()
trail.Log("catalog.sync.started",
    trail.Execution(retryID),
    trail.RetryOfExecution(executionID),
    trail.ExecutionAttempt(2),
    trail.WithExecutionSource(trail.SourceRetry),
)
```

`ParentExecution` expresses containment or causal spawning; `RetryOfExecution`
expresses retry history. Scheduling, attempt counters, locks, and next-run
calculation remain outside Trail.

### IDs

`EventID`, `FlowID`, and `ExecutionID` are distinct `[16]byte` value types. A
process-random 80-bit prefix plus a shared atomic 48-bit counter makes generation
concurrency-safe, allocation-free, and independent of wall-clock behavior. The
counter is shared by all three ID types, so values do not collide within a
process; independent process prefixes make cross-process collision negligible.

The 26-character lowercase Crockford Base32 form is only an external encoding.
The IDs do not claim ULID timestamp or ordering semantics and are not security
tokens.

### Typed fields

The primary path avoids `map[string]any`, reflection, and interface-valued
fields:

```go
trail.String("warehouse", "warehouse-a")
trail.Int("score", 97)
trail.Int64("rows", rows)
trail.Bool("matched", true)
trail.Float64("confidence", 0.97)
trail.Duration("elapsed", elapsed)
trail.Error(err) // fixed key: "error"
```

Fields retain their type and order. Duplicate keys are allowed; an individual
sink may choose its own wire representation and duplicate-key policy.

## Writer behavior

Trail starts one asynchronous writer goroutine. Defaults can be overridden
individually:

| Setting | Default |
| --- | ---: |
| `BufferCapacity` | 4096 events |
| `BatchSize` | 64 events |
| `FlushInterval` | 100 ms |
| `FullPolicy` | `trail.Drop` |

`Drop` keeps the business path non-blocking when the bounded queue is full.
`Block` waits for queue capacity and is useful when the caller explicitly values
delivery over latency. Shutdown releases blocked callers.

- Logging before `Init`, after `Close`, or while shutdown starts is a no-op and
  increments `dropped`.
- A repeated `Init` returns `trail.ErrAlreadyInitialized` and leaves the active
  writer untouched. A successful `Close` permits later initialization.
- Sink publication failures do not reach `Log`. The failed batch is discarded,
  `publish_errors` and `dropped` are incremented, and later batches continue.
- `Close` stops acceptance, drains accepted events, flushes the final batch, and
  closes the owned sink exactly once. It returns the first publishing error and
  any sink-close error. A sink must bound its own network calls if shutdown needs
  a deadline.
- After successful `Init`, the sink belongs to Trail and must not be reused or
  closed independently.

`trail.Stats()` returns inexpensive atomic counters. `Written`, `Dropped`, and
`PublishErrors` are cumulative for the process lifetime. `Buffered` is a live
gauge and returns to zero after shutdown completes. Trail does not install a
metrics framework.

## Sinks

The repository includes NDJSON stdout, concurrency-safe memory, and NATS /
JetStream sinks:

```go
import trstdout "github.com/vestavision/trail/sink/stdout"

sink := trstdout.New(os.Stdout)
```

Custom sinks implement two synchronous methods:

```go
type Sink interface {
    WriteBatch(trail.Batch) error
    Close() error
}
```

Static service metadata is supplied once per batch, not copied into each event.
The batch slices are borrowed for the duration of `WriteBatch`; a sink that keeps
them must copy them. NATS support remains outside the core package.

### NATS and JetStream

The NATS sink publishes one bounded, schema-versioned envelope per Trail batch:

```go
import trailnats "github.com/vestavision/trail/sink/nats"

sink, err := trailnats.New(existingConnection, trailnats.Config{
    Mode:    trailnats.JetStream,
    Subject: "trail.events.v1",
})
```

`New` borrows the supplied connection; `Connect` creates a connection owned by
the sink. JetStream mode waits for a publish acknowledgement. Plain NATS mode
publishes and performs a timeout-bounded flush. The sink has no retry queue and
does not add another unbounded buffer. The transport format is documented by
the `wire` package and remains separate from the core API.

## HTTP journaling

`trailhttp` wraps an outgoing transport and emits one `provider.http` event
when the response body reaches EOF or is closed:

```go
client := &http.Client{Transport: trailhttp.Wrap(http.DefaultTransport)}
req = trailhttp.WithFlow(req, flowID)
req = trailhttp.WithExecution(req, executionID)
req = trailhttp.WithEntity(req, "order", orderID)
```

Correlation attachment is explicit. The adapter uses request context only as a
private carrier and does not change Trail's core semantics. Header capture,
query inclusion, and bounded body previews are opt-in; common credential and
cookie headers are redacted by default. Callers must close response bodies, as
required by `net/http`, for completion to be recorded.

## Payloads

The `payload` package keeps large data outside event messages. It defines an
S3-compatible storage contract and structured references containing content
type, logical and stored sizes, SHA-256, compression, and retention metadata.
The filesystem and S3-compatible implementations are content-addressed. The S3
adapter supports AWS S3, MinIO, and compatible R2 configurations.
`payload.Ref.Fields(role)` attaches only searchable
reference metadata to a Trail event; it never embeds the payload itself.

`trailhttp.NewPayloadSpooler` enables opt-in, bounded request/response capture.
Accepted captures are persisted on one bounded background worker, redaction is
applied before persistence, and a full spool queue records a payload error while
still emitting the HTTP event. Close the spooler before `trail.Close` so accepted
payload jobs can attach their references.

## Proving ground and Explorer

Trail includes a storage-neutral ingestor and Explorer API, ClickHouse and
PostgreSQL adapters, and a deterministic business-story generator. The separate
[`vestavision/trail-explorer`](https://github.com/vestavision/trail-explorer)
React / TypeScript application is the reference UI. ClickHouse is the recommended reference store for high
volume and long retention; PostgreSQL is a supported, simpler choice for
moderate installations. Neither is a dependency of the core package.

Clone `trail` and `trail-explorer` as sibling directories, then start the
reference stack from the `trail` directory:

```bash
docker compose up --build
go run ./cmd/trail-generator --events 100000 --seed 42
```

Open `http://localhost:3000`. The generated dataset travels through the same
versioned JetStream envelope and durable ingestor used by applications. The
Explorer UI talks only to `/api/v1`; it has no storage-specific code.

The ingestor selects its adapter with `TRAIL_STORE=clickhouse` or
`TRAIL_STORE=postgres`. For PostgreSQL development, start the `postgres` Compose
profile and point both Go services at `TRAIL_POSTGRES_URL`. See
[PROVING_GROUND.md](PROVING_GROUND.md) for reproducible validation tiers and the
measurement checklist.

## Retention

Retention is an optional lifecycle component and is completely separate from
`trail.Init`, `trail.Log`, and `trail.Close`. Trail never starts cleanup
goroutines, opens retention database connections, or runs a scheduler as part
of the logging library.

The one-shot `trail-retention` command applies explicit ordered policies to
ClickHouse or PostgreSQL, archives every eligible batch, and only then deletes
live rows and safely garbage-collects unreferenced payloads:

```bash
trail-retention run --config retention.json --dry-run
TRAIL_ARCHIVE_FILESYSTEM_ROOT=./trail-archives trail-retention run --config retention.json
```

There is no destructive default policy and no delete-without-archive mode. A
failed archive write, verification, or catalog commit leaves live events
untouched. Archives use versioned gzip NDJSON plus a verified manifest and
support bounded, idempotent restore. PostgreSQL uses bounded transactional
deletes; ClickHouse uses bounded synchronous mutations and reclaims
physical space during later merges. Payload GC is eventually consistent and
honors `payload.Ref.RetainUntil`, `RetentionClass`, shared content-addressed
references, and configured limits. See [RETENTION.md](RETENTION.md) for policy
precedence, configuration, and operational guidance; start from
[`retention.example.json`](retention.example.json).

## Installation and development

```bash
go get github.com/vestavision/trail
```

Trail requires Go 1.24.1 or newer. The core remains standard-library-only;
optional NATS, database, and S3 adapters bring their respective client modules.

```bash
go test ./...
go test -race ./...
go vet ./...
go test -bench=. -benchmem ./...
```

Benchmarks report measured `ns/op`, `B/op`, and `allocs/op`; the project does not
encode machine-specific performance thresholds. See [BENCHMARKS.md](BENCHMARKS.md)
for the methodology, reference environment, and allocation analysis.

## License

Trail is available under the [MIT License](LICENSE).

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), the
[security policy](SECURITY.md), and the [code of conduct](CODE_OF_CONDUCT.md).
