package logscountprocessor

import (
	"context"

	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor/internal/metadata"
)

// refer to: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/filterprocessor/telemetry.go

type telemetry struct {
	attr    metric.MeasurementOption
	counter metric.Int64Counter
}

func newTelemetry(set processor.Settings) (*telemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	var counter metric.Int64Counter = telemetryBuilder.ProcessorLogsCount

	return &telemetry{
		attr:    metric.WithAttributeSet(attribute.NewSet(attribute.String(metadata.Type.String(), set.ID.String()))),
		counter: counter,
	}, nil
}

func (t *telemetry) record(ctx context.Context, cnt int64) {
	t.counter.Add(ctx, cnt, t.attr)
}
