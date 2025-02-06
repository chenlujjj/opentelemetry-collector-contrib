// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logscountprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: false}

// NewFactory returns a new factory for the Logs Count processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability))
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs) (processor.Logs, error) {
	// pCfg, ok := cfg.(*Config)
	// if !ok {
	// 	return nil, errors.New("could not initialize logs count processor")
	// }

	p, err := newProcessor(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewLogs(
		ctx,
		set,
		cfg,
		nextConsumer,
		p.processLogs,
		processorhelper.WithCapabilities(processorCapabilities),
	)
}
