// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logscountprocessor

// import "testing"
// import "go.opentelemetry.io/collector/processor/processortest"
// import "go.opentelemetry.io/collector/consumer/consumertest"
// import "github.com/stretchr/testify/require"



// func Test_newProcessor(t *testing.T) {
// 	testCases := []struct {
// 		desc        string
// 		cfg         *Config
// 		expected    *logscountProcessor
// 		expectedErr error
// 	}{
// 		{
// 			desc: "valid config",
// 			cfg: &Config{
// 				GroupByAttrs: []string{"domain", "env"},
// 			},
// 			expected: &logscountProcessor{
// 				groupKey:    "domain::env",

// 			},
// 			expectedErr: nil,
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.desc, func(t *testing.T) {
// 			logsSink := &consumertest.LogsSink{}
// 			settings := processortest.NewNopSettings()

// 			if tc.expected != nil {
// 				tc.expected.nextConsumer = logsSink
// 			}

// 			actual, err := newProcessor(tc.cfg, logsSink, )
// 			if tc.expectedErr != nil {
// 				require.ErrorContains(t, err, tc.expectedErr.Error())
// 				require.Nil(t, actual)
// 			} else {
// 				require.NoError(t, err)
// 				require.Equal(t, tc.expected.emitInterval, actual.emitInterval)
// 				require.NotNil(t, actual.aggregator)
// 				require.NotNil(t, actual.remover)
// 				require.Equal(t, tc.expected.nextConsumer, actual.nextConsumer)
// 			}
// 		})
// 	}
// }
