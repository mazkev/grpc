package tracer

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// InitTracer menginisialisasi OpenTelemetry TracerProvider yang mengirim jejak ke Jaeger via OTLP gRPC
func InitTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	// Baca alamat Jaeger dari Environment Variable atau default ke localhost:4317
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	// 1. Buat Exporter OTLP via gRPC ke Jaeger
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat otlp exporter: %w", err)
	}

	// 2. Beri identitas nama service pada jejak
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat resource tracer: %w", err)
	}

	// 3. Rakit TracerProvider dengan pengiriman berkala (Batcher)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// 4. Daftarkan secara global ke runtime OpenTelemetry
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("🔭 [OpenTelemetry] Tracer '%s' aktif mengirim jejak ke Jaeger di %s ✅", serviceName, endpoint)
	return tp, nil
}
