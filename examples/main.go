package main

import (
	"context"
	"fmt"
	"log"
	"os"

	dynatrace "github.com/Dynatrace/openfeature-provider-go"
	openfeature "github.com/open-feature/go-sdk/openfeature"
)

func main() {
	sdkKey := os.Getenv("DT_SDK_KEY")
	if sdkKey == "" {
		log.Fatal("DT_SDK_KEY environment variable is required")
	}
	options, err := dynatrace.FromSDKKey(sdkKey)
	if err != nil {
		log.Fatalf("invalid sdk key: %v", err)
	}
	provider := dynatrace.NewProvider(options)
	if err := openfeature.SetProvider(provider); err != nil {
		log.Fatalf("failed to set provider: %v", err)
	}
	client := openfeature.NewClient("my-app")
	value, err := client.BooleanValue(
		context.Background(),
		"my-flag",
		false,
		openfeature.NewEvaluationContext("user-123", nil),
	)
	if err != nil {
		log.Printf("evaluation error: %v", err)
	}
	fmt.Printf("my-flag = %v\n", value)
	openfeature.Shutdown()
}
