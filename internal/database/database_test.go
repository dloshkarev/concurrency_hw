package database

import (
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/engine/mem"
	"testing"

	"go.uber.org/zap"
)

func TestDatabase_Execute(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	parser, _ := compute.NewQueryParser(logger)
	engine := mem.NewInMemoryEngine(1000)
	db := NewDatabase(parser, engine)

	tests := []struct {
		name        string
		query       string
		want        string
		wantErr     bool
		setupFunc   func()
		cleanupFunc func()
	}{
		{
			name:  "SET command success",
			query: "SET key value",
			want:  network.SuccessCommand,
		},
		{
			name:  "GET command success",
			query: "GET key",
			want:  "[success] value",
			setupFunc: func() {
				_, _ = db.Execute("SET key value")
			},
		},
		{
			name:  "DEL command success",
			query: "DEL key",
			want:  network.SuccessCommand,
			setupFunc: func() {
				_, _ = db.Execute("SET key value")
			},
		},
		{
			name:    "Invalid command",
			query:   "INVALID key value",
			want:    "[error] cannot parse query",
			wantErr: true,
		},
		{
			name:    "Invalid SET syntax",
			query:   "SET key",
			want:    network.CannotParseQuery,
			wantErr: true,
		},
		{
			name:    "Invalid GET syntax",
			query:   "GET",
			want:    network.CannotParseQuery,
			wantErr: true,
		},
		{
			name:    "Invalid DEL syntax",
			query:   "DEL",
			want:    network.CannotParseQuery,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFunc != nil {
				tt.setupFunc()
			}

			got, err := db.Execute(tt.query)

			if (err != nil) != tt.wantErr {
				t.Errorf("Database.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("Database.Execute() = %v, want %v", got, tt.want)
			}

			if tt.cleanupFunc != nil {
				tt.cleanupFunc()
			}
		})
	}
}
