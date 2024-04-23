// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logscountprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logscountProcessor struct {
	logger *zap.Logger
	config *Config

	nextConsumer consumer.Logs
}

func newProcessor(config *Config, nextConsumer consumer.Logs, logger *zap.Logger) (*logscountProcessor, error) {
	p := &logscountProcessor{
		logger:   logger,
		config:   config,
		nextConsumer: nextConsumer,
	}
	return p, nil
}

// Capabilities returns the consumer's capabilities.
func (p *logscountProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Shutdown stops the processor.
func (p *logscountProcessor) Shutdown(ctx context.Context) error {
	p.logger.Info("logscountProcessor shutdown")

	return nil
}

// Start starts the processor.
func (p *logscountProcessor) Start(ctx context.Context, _ component.Host) error {
	p.logger.Info("logscountProcessor start")

	return nil
}

// ConsumeLogs processes the logs.
func (p *logscountProcessor) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	p.logger.Info("consume logs")

	// count log based on the group by fields
	// groupByFields := p.config.GroupByFields


	cnt := ld.LogRecordCount()
	p.logger.Info("###### count logs", zap.Int("count", cnt))
	return nil
}
