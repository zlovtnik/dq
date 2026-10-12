package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"testing"
)

func TestW3CContinuationCreatesAndExportsSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)
	defer provider.Shutdown(context.Background())
	const parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	ctx := Extract(context.Background(), parent, "test=value")
	ctx, span := Start(ctx, "worker.run")
	child, state := Fields(ctx)
	span.End()
	if child == parent || state != "test=value" {
		t.Fatalf("trace context did not continue: %s %s", child, state)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "worker.run" || spans[0].Parent.SpanID().String() != "0123456789abcdef" {
		t.Fatalf("span missing parent: %v", spans)
	}
}
