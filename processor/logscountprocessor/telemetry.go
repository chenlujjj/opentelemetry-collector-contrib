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
	attr         metric.MeasurementOption
	linesCounter metric.Int64Counter
	bytesCounter metric.Int64Counter
}

func newTelemetry(set processor.Settings) (*telemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &telemetry{
		attr:         metric.WithAttributeSet(attribute.NewSet(attribute.String(metadata.Type.String(), set.ID.String()))),
		linesCounter: telemetryBuilder.ProcessorLogsLinesTotal,
		bytesCounter: telemetryBuilder.ProcessorLogsBytesTotal,
	}, nil
}

func (t *telemetry) record(ctx context.Context, lines int64, bytes int64, attrs ...attribute.KeyValue) {
	t.linesCounter.Add(ctx, lines, t.attr, metric.WithAttributeSet(attribute.NewSet(attrs...)))
	t.bytesCounter.Add(ctx, bytes, t.attr, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}
