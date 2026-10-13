# Dynatrace OpenFeature Provider for Go

[![CI](https://github.com/Dynatrace/openfeature-provider-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Dynatrace/openfeature-provider-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Dynatrace/openfeature-provider-go.svg)](https://pkg.go.dev/github.com/Dynatrace/openfeature-provider-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/Dynatrace/openfeature-provider-go)](https://goreportcard.com/report/github.com/Dynatrace/openfeature-provider-go)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

The official [Dynatrace](https://dynatrace.com) [OpenFeature](https://openfeature.dev) provider for Go server-side applications.

The provider evaluates flags in-process: on initialization it fetches your flag configuration from the Dynatrace CDN and keeps it up to date in the background, so evaluations are local and do not make a network call.

> **Status:** Early alpha — not yet ready for production use.

## Installation

```bash
go get github.com/Dynatrace/openfeature-provider-go
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "os"

    dynatrace "github.com/Dynatrace/openfeature-provider-go"
    "github.com/open-feature/go-sdk/openfeature"
)

func main() {
    provider, err := dynatrace.NewProvider(os.Getenv("DT_SDK_KEY"))
    if err != nil {
        panic(err)
    }

    openfeature.SetProviderAndWait(provider)

    client := openfeature.NewClient("my-app")
    enabled, _ := client.BooleanValue(
        context.Background(),
        "my-flag",
        false,
        openfeature.NewEvaluationContext("user-123", nil),
    )
    fmt.Println("flag value:", enabled)

    // On shutdown:
    openfeature.Shutdown()
}
```

Once the provider is registered, all flag evaluation goes through the OpenFeature API. See the
[OpenFeature Go SDK documentation](https://openfeature.dev/docs/reference/technologies/server/go/)
for evaluation contexts, hooks, events, and the rest of the SDK surface.

## Configuration

Pass options as variadic arguments to `NewProvider`:

```go
provider, err := dynatrace.NewProvider(
    sdkKey,
    dynatrace.WithConfigOrigin("https://cdn.eu.fm.dynatracelabs.com"),
    dynatrace.WithMetricsOrigin("https://metrics.eu.fm.dynatracelabs.com"),
)
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `WithConfigOrigin(origin)` | `string` | derived from SDK key | Override the flag configuration origin. |
| `WithMetricsOrigin(origin)` | `string` | derived from SDK key | Override the evaluation metrics ingest origin. |

A runnable example lives in [`examples`](examples).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

### Running the e2e tests

The cross-provider Gherkin suite lives in the [`fm-provider-test-harness`](https://github.com/Dynatrace/fm-provider-test-harness) submodule and runs against its mock server via Testcontainers, so Docker must be running:

```bash
make e2e
```

Set `SKIP_HARNESS_UPDATE=1` to keep a local harness checkout, and `MOCKSERVER_IMAGE` to run against a locally built mock server image.

## License

Apache 2.0 — see [LICENSE](LICENSE).
