.PHONY: test vet lint tidy

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy
