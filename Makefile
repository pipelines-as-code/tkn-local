.PHONY: build test e2e fmt check

build:
	go build -o bin/tkn-local ./cmd/tkn-local

test:
	go test -race ./...

e2e:
	go test -tags=e2e -v -timeout=15m ./test/e2e

fmt:
	gofumpt -w -extra cmd internal pkg test

check:
	go vet ./...
	go test -race ./...
