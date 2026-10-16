# Contributing

## Prerequisites

- Go 1.25+
- [golangci-lint](https://golangci-lint.run/)

## Development

```bash
# Run tests
go test ./...

# Run tests with race detector
go test -race ./...

# Lint
golangci-lint run
```

## Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
PR titles must follow the format: `type: description` (e.g., `feat: add SSE support`).

Name branches `<type>/<slug>` (e.g., `feat/sse-client`).

## Architecture

The provider wraps `flagd-core` (`github.com/open-feature/flagd/core`) for in-process evaluation
and adds HTTP config fetching, polling, retry, and OpenFeature lifecycle management.

<!-- TODO: link to public provider architecture docs once available -->

### Public API

| File | Purpose |
| --- | --- |
| `provider.go` | `DynatraceProvider` struct, `NewProvider`, `ProviderOption` functional options |
| `options.go` | SDK key validation and CDN / metrics URL derivation |

### Internal implementation

| File | Purpose |
| --- | --- |
| `internal/fetch.go` | CDN HTTP fetcher — ETag, Last-Modified, stale-node defense, 429 handling |
| `internal/poll.go` | Fixed-rate polling goroutine with context-aware cancellation |
| `internal/retry.go` | Exponential-backoff retry strategy with jitter |
