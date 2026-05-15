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

## Architecture

The provider wraps `flagd-core` (`github.com/open-feature/flagd/core`) for in-process evaluation
and adds HTTP config fetching, polling, retry, and OpenFeature lifecycle management.

See `~/git/Dynatrace/fm-knowledge-base/docs/product/architecture/providers.md` for the authoritative spec.

| File | Purpose |
|------|---------|
| `options.go` | SDK key validation + CDN URL derivation |
| `retry.go` | Exponential-backoff retry strategy |
| `fetch.go` | CDN HTTP fetcher with ETag + stale-node defense |
| `poll.go` | Fixed-delay polling goroutine |
| `provider.go` | OpenFeature provider + state machine + event emission |
