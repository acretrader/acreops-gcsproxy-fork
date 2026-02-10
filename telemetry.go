package main

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer = otel.Tracer("kepler")
	meter  = otel.Meter("kepler")
)

// Metric instruments — initialized in initTelemetry.
var (
	bytesServed   metric.Int64Counter
	rangeRequests metric.Int64Counter
	panicCounter  metric.Int64Counter
)

func initTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	// W3C TraceContext propagation (Traefik sends traceparent)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Resource with fallback service name (OTEL_SERVICE_NAME env overrides)
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(semconv.ServiceName("kepler")),
	)
	if err != nil {
		return nil, err
	}

	// Trace exporter — OTLP over HTTP, reads OTEL_EXPORTER_OTLP_ENDPOINT from env
	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// Metric exporter — OTLP over HTTP
	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// Initialize metric instruments
	bytesServed, err = meter.Int64Counter("kepler.bytes_served",
		metric.WithDescription("Total bytes served to clients"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	rangeRequests, err = meter.Int64Counter("kepler.range_requests",
		metric.WithDescription("Range request count by type"),
	)
	if err != nil {
		return nil, err
	}

	panicCounter, err = meter.Int64Counter("kepler.panics",
		metric.WithDescription("Total panic recoveries"),
	)
	if err != nil {
		return nil, err
	}

	shutdown = func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}
	return shutdown, nil
}

func extractTraceContext(ctx context.Context) (traceID, spanID string) {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	if sc.HasTraceID() {
		traceID = sc.TraceID().String()
	}
	if sc.HasSpanID() {
		spanID = sc.SpanID().String()
	}
	return
}
