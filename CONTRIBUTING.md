# Contributing to Trail

Thank you for helping improve Trail. Keep changes small, explicit, bounded, and
consistent with the project's stateless correlation model.

## Before opening a change

- Open an issue before large API, wire-format, storage-schema, or behavioral changes.
- Do not introduce OpenTelemetry concepts, mandatory context propagation, unbounded
  buffering, or storage-specific behavior into the core package.
- Preserve backwards compatibility unless the change has been discussed and documented.

## Development

Trail requires Go 1.24.1 or newer.

```bash
go test ./...
go test -race ./...
go vet ./...
go test -bench=. -benchmem ./...
```

Add focused tests for behavior changes. Benchmark hot-path changes and report the
machine, OS, Go version, command, and raw results. Do not commit generated benchmark
claims or machine-specific thresholds.

## Pull requests

- Use a focused title and explain the motivation and observable behavior.
- Call out public API, wire-format, schema, allocation, and shutdown implications.
- Update documentation and examples when user-facing behavior changes.
- Confirm that no credentials, private payloads, or production data are included.

By contributing, you agree that your contribution is licensed under the MIT License.
