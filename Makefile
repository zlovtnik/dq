GO ?= go

.PHONY: check test build ui-check benchmark
check:
	@test -z "$$(gofmt -l $$(find cmd internal pkg -name '*.go' -not -name '*.pb.go'))"
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
