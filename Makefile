GO ?= go
PYTHON ?= python3

.PHONY: check test build ui-check benchmark acceptance
check:
	@command -v rg >/dev/null 2>&1 || { echo "rg is required for the gofmt check" >&2; exit 1; }
	@test -z "$$(gofmt -l $$(rg --files cmd internal pkg processors tests -g '*.go' | rg -v '\.pb\.go$$'))"
	$(GO) vet ./...
	$(GO) test -race ./...
test:
	$(GO) test ./...
build:
	$(GO) build -o /tmp/dq-manager ./cmd/manager
	$(GO) build -o /tmp/dq-worker ./cmd/worker
ui-check:
	cd ui && bun run test && bun run build
benchmark:
	$(GO) test -run '^$$' -bench . -benchmem ./...
acceptance:
	$(PYTHON) tests/acceptance/run.py
