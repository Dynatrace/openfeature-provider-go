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
	_ = os.Getenv("DT_SDK_KEY") // placeholder; full init coming in PR 4
	provider := dynatrace.NewProvider()
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
	if err := openfeature.ShutdownWithContext(context.Background()); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
