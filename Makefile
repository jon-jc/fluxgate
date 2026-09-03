# Fluxgate developer workflow.
#
# Every target here is also what CI runs, so a green `make ci` locally means a
# green pipeline -- there is no second, hidden set of commands to keep in sync.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE      := github.com/jon-jc/fluxgate
BIN_DIR     := bin
CMDS        := $(notdir $(wildcard cmd/*))

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

VERSION_PKG := $(MODULE)/internal/version
LDFLAGS     := -s -w \
	-X '$(VERSION_PKG).version=$(VERSION)' \
	-X '$(VERSION_PKG).commit=$(COMMIT)' \
	-X '$(VERSION_PKG).date=$(BUILD_DATE)'

GO          ?= go
GOFLAGS     ?=

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make <target>\n\nTargets:\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

.PHONY: build
build: $(addprefix build-,$(CMDS)) ## Build every command into bin/

.PHONY: build-%
build-%:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$* ./cmd/$*

.PHONY: run
run: ## Run the ingest API against the local defaults
	LOG_FORMAT=text ENVIRONMENT=local $(GO) run ./cmd/ingest-api

.PHONY: test
test: ## Run the unit test suite with the race detector
	$(GO) test -race -count=1 -timeout 5m ./...

.PHONY: test-short
test-short: ## Run tests without the race detector (no cgo required)
	$(GO) test -count=1 -timeout 5m ./...

.PHONY: cover
cover: ## Produce coverage.out and print a per-package summary
	$(GO) test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out | tail -n 20

.PHONY: cover-html
cover-html: cover ## Open the coverage report in a browser
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

.PHONY: bench
bench: ## Run benchmarks
	$(GO) test -run '^$$' -bench . -benchmem ./...

.PHONY: fmt
fmt: ## Format all Go source
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w -local $(MODULE) .
	gofmt -w .

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (installs it on demand)
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found; install from https://golangci-lint.run"; exit 1; }
	golangci-lint run

.PHONY: vulncheck
vulncheck: ## Report known vulnerabilities in dependencies
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: tidy
tidy: ## Tidy and verify go.mod
	$(GO) mod tidy
	$(GO) mod verify

.PHONY: ci
ci: tidy vet test ## Everything CI enforces, minus the linter binary

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html
