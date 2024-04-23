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

	consumer consumer.Logs

	// pipe          *pipeline.DirectedPipeline
	// firstOperator operator.Operator
	// emitter       *adapter.LogEmitter
	// converter     *adapter.Converter
	// fromConverter *adapter.FromPdataConverter
	// wg            sync.WaitGroup
	// shutdownFns   []component.ShutdownFunc
}

func newProcessor(config *Config, nextConsumer consumer.Logs, logger *zap.Logger) (*logscountProcessor, error) {
	p := &logscountProcessor{
		logger:   logger,
		config:   config,
		consumer: nextConsumer,
	}

	// baseCfg := p.config.BaseConfig

	// p.emitter = adapter.NewLogEmitter(p.logger.Sugar())
	// pipe, err := pipeline.Config{
	// 	Operators:     baseCfg.Operators,
	// 	DefaultOutput: p.emitter,
	// }.Build(p.logger.Sugar())
	// if err != nil {
	// 	return nil, err
	// }

	// p.pipe = pipe

	return p, nil
}

func (ltp *logscountProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (ltp *logscountProcessor) Shutdown(ctx context.Context) error {
	fmt.Println("logscountProcessor shutdown")

	// for _, fn := range ltp.shutdownFns {
	// 	if err := fn(ctx); err != nil {
	// 		return err
	// 	}
	// }
	// ltp.wg.Wait()

	return nil
}

func (ltp *logscountProcessor) Start(ctx context.Context, _ component.Host) error {
	fmt.Println("logscountProcessor start")

	// // There is no need for this processor to use storage
	// err := ltp.pipe.Start(storage.NewNopClient())
	// if err != nil {
	// 	return err
	// }
	// ltp.shutdownFns = append(ltp.shutdownFns, func(ctx context.Context) error {
	// 	ltp.logger.Info("Stopping logs transform processor")
	// 	return ltp.pipe.Stop()
	// })

	// pipelineOperators := ltp.pipe.Operators()
	// if len(pipelineOperators) == 0 {
	// 	return errors.New("processor requires at least one operator to be configured")
	// }
	// ltp.firstOperator = pipelineOperators[0]

	// wkrCount := int(math.Max(1, float64(runtime.NumCPU())))

	// ltp.converter = adapter.NewConverter(ltp.logger)
	// ltp.converter.Start()
	// ltp.shutdownFns = append(ltp.shutdownFns, func(ctx context.Context) error {
	// 	ltp.converter.Stop()
	// 	return nil
	// })

	// ltp.fromConverter = adapter.NewFromPdataConverter(wkrCount, ltp.logger)
	// ltp.fromConverter.Start()
	// ltp.shutdownFns = append(ltp.shutdownFns, func(ctx context.Context) error {
	// 	ltp.fromConverter.Stop()
	// 	return nil
	// })
	// // Below we're starting 3 loops:
	// // * first which reads all the logs translated by the fromConverter and then forwards
	// //   them to pipeline
	// // ...
	// ltp.wg.Add(1)
	// go ltp.converterLoop(ctx)

	// // * second which reads all the logs modified by the pipeline and then forwards
	// //   them to converter
	// // ...
	// ltp.wg.Add(1)
	// go ltp.emitterLoop(ctx)

	// // ...
	// // * third which reads all the logs produced by the converter
	// //   (aggregated by Resource) and then places them on the next consumer
	// ltp.wg.Add(1)
	// go ltp.consumerLoop(ctx)
	return nil
}

func (ltp *logscountProcessor) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	fmt.Println("consume logs")
	return nil
	// // Add the logs to the chain
	// return ltp.fromConverter.Batch(ld)
}

// converterLoop reads the log entries produced by the fromConverter and sends them
// into the pipeline
// func (ltp *logscountProcessor) converterLoop(ctx context.Context) {
// 	defer ltp.wg.Done()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			ltp.logger.Debug("converter loop stopped")
// 			return

// 		case entries, ok := <-ltp.fromConverter.OutChannel():
// 			if !ok {
// 				ltp.logger.Debug("fromConverter channel got closed")
// 				return
// 			}

// 			for _, e := range entries {
// 				// Add item to the first operator of the pipeline manually
// 				if err := ltp.firstOperator.Process(ctx, e); err != nil {
// 					ltp.logger.Error("processor encountered an issue with the pipeline", zap.Error(err))
// 					break
// 				}
// 			}
// 		}
// 	}
// }

// // emitterLoop reads the log entries produced by the emitter and batches them
// // in converter.
// func (ltp *logscountProcessor) emitterLoop(ctx context.Context) {
// 	defer ltp.wg.Done()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			ltp.logger.Debug("emitter loop stopped")
// 			return
// 		case e, ok := <-ltp.emitter.OutChannel():
// 			if !ok {
// 				ltp.logger.Debug("emitter channel got closed")
// 				return
// 			}

// 			if err := ltp.converter.Batch(e); err != nil {
// 				ltp.logger.Error("processor encountered an issue with the converter", zap.Error(err))
// 			}
// 		}
// 	}
// }

// // consumerLoop reads converter log entries and calls the consumer to consumer them.
// func (ltp *logscountProcessor) consumerLoop(ctx context.Context) {
// 	defer ltp.wg.Done()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			ltp.logger.Debug("consumer loop stopped")
// 			return

// 		case pLogs, ok := <-ltp.converter.OutChannel():
// 			if !ok {
// 				ltp.logger.Debug("converter channel got closed")
// 				return
// 			}

// 			if err := ltp.consumer.ConsumeLogs(ctx, pLogs); err != nil {
// 				ltp.logger.Error("processor encountered an issue with next consumer", zap.Error(err))
// 			}
// 		}
// 	}
// }
