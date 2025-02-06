// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logscountprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor"

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

const separator = "::"

type logscountProcessor struct {
	logger   *zap.Logger
	config   *Config
	groupKey string

	telemetry *telemetry
}

type logsSize struct {
	lines int
	bytes int
}

func newProcessor(config *Config, set processor.Settings) (*logscountProcessor, error) {
	telemetry, err := newTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating logs count processor telemetry: %w", err)
	}

	groupKey := strings.Join(config.GroupByAttrs, separator)

	p := &logscountProcessor{
		logger:    set.Logger,
		config:    config,
		groupKey:  groupKey,
		telemetry: telemetry,
	}

	p.logger.Info("##### create logscountProcessor", zap.String("groupKey", groupKey))

	return p, nil
}

// // Shutdown stops the processor.
// func (p *logscountProcessor) Shutdown(ctx context.Context) error {
// 	p.logger.Info("logscountProcessor shutdown")
// 	return nil
// }

// // Start starts the processor.
// func (p *logscountProcessor) Start(ctx context.Context, _ component.Host) error {
// 	p.logger.Info("logscountProcessor start")
// 	return nil
// }

func (p *logscountProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	p.logger.Info("logscountProcessor consume logs")

	total1 := ld.LogRecordCount()

	counter := make(map[string]logsSize) // key is the value of groupKey, value is the size

	groupValues := make([]string, len(p.config.GroupByAttrs))
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		attrs := rl.Resource().Attributes()
		for idx, attr := range p.config.GroupByAttrs {
			val, ok := attrs.Get(attr)
			if !ok {
				groupValues[idx] = "unknown"
			} else {
				groupValues[idx] = val.AsString()
			}
		}
		groupValue := strings.Join(groupValues, separator)

		size := resourceLogsSize(rl)
		if _, ok := counter[groupValue]; !ok {
			counter[groupValue] = size
		} else {
			orig := counter[groupValue]
			counter[groupValue] = logsSize{lines: orig.lines + size.lines, bytes: orig.bytes + size.bytes}
		}
	}

	total2 := 0
	kvs := make([]attribute.KeyValue, len(p.config.GroupByAttrs))
	for groupVal, cnt := range counter {
		total2 += cnt.lines

		zapFields := make([]zap.Field, 0, len(p.config.GroupByAttrs)+2)
		for i := range p.config.GroupByAttrs {
			attr := p.config.GroupByAttrs[i]
			value := strings.Split(groupVal, separator)[i]
			zapFields = append(zapFields, zap.String(decorateGroupAttr(attr), value))

			kvs[i] = attribute.String(decorateGroupAttr(attr), value)
		}
		zapFields = append(zapFields, zap.Int("lines", cnt.lines), zap.Int("bytes", cnt.bytes))

		p.logger.Info("###### count logs", zapFields...)

		p.telemetry.record(ctx, int64(cnt.lines), int64(cnt.bytes), kvs...)
	}

	p.logger.Info("##### total logs", zap.Int("total1", total1), zap.Int("total2", total2))

	fmt.Printf("##### total logs, total1: %d, total2: %d\n", total1, total2)


	// p.logger.Info("##### total logs", zap.Int("total", total1))

	return ld, nil
}

// NOTE: splunk log field name cannot start with "_"
func decorateGroupAttr(attr string) string {
	return fmt.Sprintf("%s__", attr)
}

func resourceLogsSize(rl plog.ResourceLogs) logsSize {
	bytes := attributesSize(rl.Resource().Attributes())
	lines := 0
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		size := scopeLogsSize(rl.ScopeLogs().At(i))
		bytes += size.bytes
		lines += size.lines
	}
	return logsSize{lines: lines, bytes: bytes}
}

func scopeLogsSize(sl plog.ScopeLogs) logsSize {
	bytes := attributesSize(sl.Scope().Attributes())
	for i := 0; i < sl.LogRecords().Len(); i++ {
		bytes += logRecordSize(sl.LogRecords().At(i))
	}
	return logsSize{lines: sl.LogRecords().Len(), bytes: bytes}
}

func logRecordSize(lr plog.LogRecord) int {
	res := 8 + 8 + 4 // timestamp + observedTimestamp + severity number
	res += len(lr.SeverityText())
	res += valueSize(lr.Body())
	res += 4 // flags
	if !lr.TraceID().IsEmpty() {
		res += 16
	}
	if !lr.SpanID().IsEmpty() {
		res += 8
	}
	res += attributesSize(lr.Attributes())
	return res
}

func attributesSize(attrs pcommon.Map) int {
	s := 0
	attrs.Range(func(k string, v pcommon.Value) bool {
		s += len(k) + valueSize(v)
		return true
	})
	return s
}

func valueSize(v pcommon.Value) int {
	return len(v.AsString())
}
