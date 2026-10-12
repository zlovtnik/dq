// Package telemetry initializes optional OTLP tracing and W3C propagation.
package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"os"
)

var propagator = propagation.TraceContext{}

func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", serviceName))), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1)))}
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		options = append(options, sdktrace.WithSampler(sdktrace.NeverSample()))
	} else if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(2048), sdktrace.WithMaxExportBatchSize(256)))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagator)
	return provider.Shutdown, nil
}
func Extract(ctx context.Context, traceparent, tracestate string) context.Context {
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent, "tracestate": tracestate})
}
func Fields(ctx context.Context) (string, string) {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier["traceparent"], carrier["tracestate"]
}
func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("delip-queve").Start(ctx, name)
}
func Fail(span trace.Span) { span.SetStatus(codes.Error, "operation_failed") }
