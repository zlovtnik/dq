# Build from the repository root; use target manager or worker.
FROM oven/bun:1.4.0 AS ui
WORKDIR /src
COPY services/dq/ui/package.json services/dq/ui/bun.lock ./
RUN bun install --frozen-lockfile
COPY services/dq/ui ./
RUN bun run build

FROM golang:1.27-bookworm AS go
WORKDIR /src
COPY services/dq/go.mod services/dq/go.sum ./
RUN go mod download
COPY services/dq ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dq-manager ./cmd/manager
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dq-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot AS manager
WORKDIR /app
COPY --from=go /out/dq-manager /usr/local/bin/dq-manager
COPY --from=ui /src/dist /app/ui/dist
COPY --from=ui /src/remotes-dist /app/ui/remotes-dist
USER nonroot:nonroot
EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/dq-manager"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker
WORKDIR /app
COPY --from=go /out/dq-worker /usr/local/bin/dq-worker
USER nonroot:nonroot
EXPOSE 9091 8091
ENTRYPOINT ["/usr/local/bin/dq-worker"]
