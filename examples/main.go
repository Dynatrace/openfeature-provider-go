package main

import (
	"context"
	"fmt"
	"log"
	"os"

	dynatrace "github.com/Dynatrace/openfeature-provider-go"
	openfeature "github.com/open-feature/go-sdk/openfeature"
)

// Usage:
//
//	DT_SDK_KEY=dt01.server_... \
//	DT_CDN_BASE_URL=https://cdn.eu.fm.dynatracelabs.com/server/ \  # optional; omit for prod
//	DT_TARGETING_KEY=my-user \
//	DT_EMAIL=me@dynatrace.com \
//	go run ./examples
func main() {
	sdkKey := os.Getenv("DT_SDK_KEY")
	if sdkKey == "" {
		log.Fatal("DT_SDK_KEY is required")
	}

	var opts []dynatrace.ProviderOption
	if base := os.Getenv("DT_CDN_BASE_URL"); base != "" {
		opts = append(opts, dynatrace.WithCDNBaseURL(base))
	}

	provider, err := dynatrace.NewProvider(sdkKey, opts...)
	if err != nil {
		log.Fatalf("invalid sdk key: %v", err)
	}

	if err := openfeature.SetProviderAndWait(provider); err != nil {
		log.Fatalf("provider init failed: %v", err)
	}
	fmt.Println("provider: READY")

	client := openfeature.NewClient("example")
	ctx := context.Background()

	fmt.Println("\n--- without targeting context ---")
	eval(client, ctx, openfeature.NewEvaluationContext("", nil))

	fmt.Printf("\n--- with targeting key=%s email=%s ---\n",
		os.Getenv("DT_TARGETING_KEY"), os.Getenv("DT_EMAIL"))
	eval(client, ctx, openfeature.NewEvaluationContext(
		os.Getenv("DT_TARGETING_KEY"),
		map[string]any{"email": os.Getenv("DT_EMAIL")},
	))

	openfeature.Shutdown()
}

func eval(client *openfeature.Client, ctx context.Context, evalCtx openfeature.EvaluationContext) {
	for _, flag := range []string{"boolean-flag"} {
		d, _ := client.BooleanValueDetails(ctx, flag, false, evalCtx)
		fmt.Printf("  %-25s bool    = %-5v variant=%-10q reason=%s\n", flag, d.Value, d.Variant, d.Reason)
	}
	for _, flag := range []string{"string-flag"} {
		d, _ := client.StringValueDetails(ctx, flag, "", evalCtx)
		fmt.Printf("  %-25s string  = %-10q variant=%-10q reason=%s\n", flag, d.Value, d.Variant, d.Reason)
	}
	for _, flag := range []string{"number-flag"} {
		d, _ := client.FloatValueDetails(ctx, flag, 0, evalCtx)
		fmt.Printf("  %-25s float   = %-5v variant=%-10q reason=%s\n", flag, d.Value, d.Variant, d.Reason)
	}
}
