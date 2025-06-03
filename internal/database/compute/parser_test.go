//go:build unit

package compute_test

import (
	"concurrency_hw/internal/database/compute"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"testing"
)

func TestNewQueryParser(t *testing.T) {
	tests := []struct {
		name    string
		logger  *zap.Logger
		wantErr bool
	}{
		{"Valid logger", zaptest.NewLogger(t), false},
		{"Nil logger", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compute.NewQueryParser(tt.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewQueryParser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestQueryParser_ParseQuery(t *testing.T) {
	logger := zaptest.NewLogger(t)
	parser, _ := compute.NewQueryParser(logger)

	tests := []struct {
		name      string
		query     string
		wantQuery compute.Query
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "Valid SET command",
			query:     "SET key value",
			wantQuery: compute.Query{CommandId: compute.SetCommandId, Args: []string{"key", "value"}},
			wantErr:   false,
		},
		{
			name:      "Valid GET command",
			query:     "GET key",
			wantQuery: compute.Query{CommandId: compute.GetCommandId, Args: []string{"key"}},
			wantErr:   false,
		},
		{
			name:      "Valid DEL command",
			query:     "DEL key",
			wantQuery: compute.Query{CommandId: compute.DelCommandId, Args: []string{"key"}},
			wantErr:   false,
		},
		{
			name:    "Empty query",
			query:   "",
			wantErr: true,
			errMsg:  "no tokens found",
		},
		{
			name:    "Invalid command",
			query:   "INVALID key",
			wantErr: true,
			errMsg:  "invalid command token: INVALID",
		},
		{
			name:    "SET with missing arguments",
			query:   "SET key",
			wantErr: true,
			errMsg:  "invalid count of arguments",
		},
		{
			name:    "GET with too many arguments",
			query:   "GET key extra",
			wantErr: true,
			errMsg:  "invalid count of arguments",
		},
		{
			name:    "DEL with too many arguments",
			query:   "DEL key extra",
			wantErr: true,
			errMsg:  "invalid count of arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parser.ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err.Error() != tt.errMsg {
					t.Errorf("ParseQuery() error message = %v, want %v", err.Error(), tt.errMsg)
				}
				return
			}
			if got.CommandId != tt.wantQuery.CommandId {
				t.Errorf("ParseQuery() = %+v, want %+v", got, tt.wantQuery)
			}
			assert.ElementsMatch(t, got.Args, tt.wantQuery.Args)
		})
	}
}
