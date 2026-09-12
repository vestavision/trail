# Benchmarks

Trail benchmarks measure the cost of accepting events into the bounded writer.
They do not include a network sink unless the benchmark name explicitly says
so.

Run the complete benchmark suite with:

```bash
go test -run='^$' -bench=. -benchmem -count=5 ./...
```

Use the same Go version, machine, power profile, and `GOMAXPROCS` when comparing
results. Report all runs (or use `benchstat`) rather than selecting the fastest
one. Benchmark numbers are observations, not compatibility guarantees or CI
thresholds.

## Reference environment

The initial measurements were recorded on an Apple M2 (`darwin/arm64`). The
repository targets Go 1.24 and newer. Always record the output of `go version`,
`go env GOOS GOARCH`, the operating-system version, processor, logical CPU
count, date, and exact benchmark command alongside new results.

Initial Apple M2 reference:

```text
BenchmarkLogNoFields-8       ~291 ns/op    0 B/op    0 allocs/op
BenchmarkLogThreeFields-8    ~359 ns/op  144 B/op    1 allocs/op
BenchmarkNewFlow-8           ~7.5 ns/op    0 B/op    0 allocs/op
BenchmarkNewExecution-8      ~7.5 ns/op    0 B/op    0 allocs/op
BenchmarkConcurrentLog-8     ~627 ns/op    0 B/op    0 allocs/op
```

## Three-field allocation

The single 144-byte allocation in `BenchmarkLogThreeFields` is the backing
array created for three `Field` values. A `Field` occupies 48 bytes on arm64.
The array must outlive `Log` because the event crosses the asynchronous queue.
Compiler escape analysis identifies the allocation at the `make([]Field, ...)`
in `Log`; the variadic option arguments are not the cause.

Removing it would currently require a materially less readable specialized API,
a larger queue element for every event, a custom queue, or field pooling with
additional contention and object-retention rules. Trail intentionally keeps the
one allocation for field-bearing events until application profiling demonstrates
that one of those tradeoffs is worthwhile. No-field logging and ID generation
remain allocation-free.

To reproduce the allocation investigation:

```bash
go test -run='^$' -bench=BenchmarkLogThreeFields -benchmem \
  -memprofile=three-fields.mem .
go tool pprof -top -alloc_space three-fields.mem
go test -run='^$' -gcflags='-m=2' .
```
