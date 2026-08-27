# Dynatrace OpenFeature Provider for Go

[![CI](https://github.com/Dynatrace/openfeature-provider-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Dynatrace/openfeature-provider-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Dynatrace/openfeature-provider-go.svg)](https://pkg.go.dev/github.com/Dynatrace/openfeature-provider-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/Dynatrace/openfeature-provider-go)](https://goreportcard.com/report/github.com/Dynatrace/openfeature-provider-go)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A server-side [OpenFeature](https://openfeature.dev) provider for Go that evaluates feature flags
in-process using configuration fetched from the Dynatrace CDN.

## Features

- In-process evaluation via [flagd-core](https://github.com/open-feature/flagd) — no external gRPC/RPC calls during evaluation
- Conditional CDN fetching with ETag and Last-Modified (stale-node defense per §5.7)
- Automatic polling every 10 seconds
- Exponential-backoff retry on 5xx errors (5 attempts, base 100ms, 2× multiplier, +0–20% jitter)
- 429 / Retry-After rate-limit support
- Provider state machine: NOT_READY → READY → STALE → ERROR (1-minute grace period)
- Emits `PROVIDER_READY`, `PROVIDER_STALE`, `PROVIDER_ERROR`, `PROVIDER_CONFIGURATION_CHANGED`

## Installation

```bash
go get github.com/Dynatrace/openfeature-provider-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "os"

    dynatrace "github.com/Dynatrace/openfeature-provider-go"
    openfeature "github.com/open-feature/go-sdk/openfeature"
)

func main() {
    provider, err := dynatrace.NewProvider(os.Getenv("DT_SDK_KEY"))
    if err != nil {
        panic(err)
    }

    openfeature.SetProviderAndWait(provider)

    client := openfeature.NewClient("my-app")
    value, _ := client.BooleanValue(
        context.Background(),
        "my-flag",
        false,
        openfeature.NewEvaluationContext("user-123", nil),
    )
    fmt.Println("flag value:", value)

    openfeature.Shutdown()
}
```

## Configuration

### SDK Key

Obtain a Server SDK key from the Dynatrace Feature Management UI. Keys have the format:

```text
dt01.server_{geo}_{rand}.{secret}_{sha}
```

Use `options.PublicKey()` for logging — it strips the private segment.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithConfigOrigin(origin)` | derived from key | Override config origin (scheme, host, port; no path) for dev/labs environments |
| `WithMetricsOrigin(origin)` | derived from key | Override metrics origin (scheme, host, port; no path) for dev/labs environments. The provider appends `/v1/metrics`. |

A metrics origin is scheme, host, and optional port only. Passing a path (including the old full ingest URL `/v1/metrics`) is rejected; the provider always appends `/v1/metrics` itself.

### Polling and Grace Period

Per the [provider specification](https://github.com/Dynatrace/fm-knowledge-base/blob/main/docs/product/architecture/providers.md):

- **Poll interval**: 10 seconds (not user-configurable)
- **Grace period**: 60 seconds before STALE transitions to ERROR (not user-configurable)

## State Machine

```text
NOT_READY → READY    : successful Init
READY → STALE        : poll failure
STALE → READY        : successful poll (recovery)
STALE → ERROR        : grace period (60s) expires
ERROR → READY        : successful poll (recovery)
* → NOT_READY        : Shutdown
```

## License

Apache-2.0. See [LICENSE](LICENSE).
