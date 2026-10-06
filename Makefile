.PHONY: test e2e vet lint tidy

test:
	go test -race -count=1 ./...

# requires docker; runs the fm-provider-test-harness gherkin suite against its mock server
e2e:
	git submodule update --init fm-provider-test-harness
	cd e2e && go test -count=1 -timeout 20m ./...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy
