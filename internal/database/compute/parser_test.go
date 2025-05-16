package compute

import (
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
			_, err := NewQueryParser(tt.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewQueryParser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestQueryParser_ParseQuery(t *testing.T) {
	logger := zaptest.NewLogger(t)
	parser, _ := NewQueryParser(logger)

	tests := []struct {
		name      string
		query     string
		wantQuery Query
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "Valid SET command",
			query:     "SET key value",
			wantQuery: Query{commandId: SetCommandId, args: []string{"key", "value"}},
			wantErr:   false,
		},
		{
			name:      "Valid GET command",
			query:     "GET key",
			wantQuery: Query{commandId: GetCommandId, args: []string{"key"}},
			wantErr:   false,
		},
		{
			name:      "Valid DEL command",
			query:     "DEL key",
			wantQuery: Query{commandId: DelCommandId, args: []string{"key"}},
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
			errMsg:  "SET: invalid count of arguments",
		},
		{
			name:    "GET with too many arguments",
			query:   "GET key extra",
			wantErr: true,
			errMsg:  "GET: invalid count of arguments",
		},
		{
			name:    "DEL with too many arguments",
			query:   "DEL key extra",
			wantErr: true,
			errMsg:  "DEL: invalid count of arguments",
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
			if got.commandId != tt.wantQuery.commandId {
				t.Errorf("ParseQuery() = %+v, want %+v", got, tt.wantQuery)
			}
			assert.ElementsMatch(t, got.args, tt.wantQuery.args)
		})
	}
}
