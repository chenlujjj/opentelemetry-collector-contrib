// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logscountprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logscountprocessor"

import (
	"errors"

	"go.opentelemetry.io/collector/component"

)

const (
	domainField = "domain"

)

// Config is the config of the processor.
type Config struct {
	// count group by fields
	GroupByFields []string `mapstructure:"group_by_fields"`
}

var _ component.Config = (*Config)(nil)


// createDefaultConfig returns the default config for the processor.
func createDefaultConfig() component.Config {
	return &Config{
		GroupByFields: []string{domainField},
	}
}

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.GroupByFields) == 0 {
		return errors.New("group_by_fields must not be empty")
	}
	return nil
}
