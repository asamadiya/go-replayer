GO      ?= go
BINDIR  ?= bin
CMDS    := go-replayer grpc-dummy-target grpc-gap-target grpc-parity-check

.PHONY: all build test race cover lint fmt fmt-check vet tidy clean

all: fmt-check vet test build

build:
	$(GO) build -o $(BINDIR)/go-replayer ./
	$(GO) build -o $(BINDIR)/grpc-dummy-target ./cmd/grpc-dummy-target
	$(GO) build -o $(BINDIR)/grpc-gap-target ./cmd/grpc-gap-target
	$(GO) build -o $(BINDIR)/grpc-parity-check ./cmd/grpc-parity-check

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

lint:
	golangci-lint run

fmt:
	gofmt -w .

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "Not gofmt-clean:"; echo "$$unformatted"; exit 1; fi

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BINDIR) coverage.out
