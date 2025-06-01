package database_test

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/creator"
	"concurrency_hw/internal/database/network"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	"go.uber.org/zap"
)

func TestDatabase_Execute(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	conf := config.Load()

	initializer := creator.NewCreator(logger, conf)

	db := initializer.CreateDatabase()

	tmpWal, err := initializer.CreateWal()
	if err != nil {
		t.Fatal(err)
	}

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
			query: "SET key1 value1",
			want:  network.SuccessCommand,
		},
		{
			name:  "GET command success",
			query: "GET key2",
			want:  "[success] value2",
			setupFunc: func() {
				_, _ = db.Execute("SET key2 value2")
			},
		},
		{
			name:  "DEL command success",
			query: "DEL key3",
			want:  network.SuccessCommand,
			setupFunc: func() {
				_, _ = db.Execute("SET key3 value3")
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

	expected := map[int]string{
		0: "SET key1 value1",
		1: "SET key2 value2",
		2: "SET key3 value3",
		3: "DEL key3",
	}

	var idx int
	_ = tmpWal.ForEach(func(queryString string) error {
		assert.Equal(t, queryString, expected[idx])
		idx++
		return nil
	})

	err = os.RemoveAll(conf.WalConfig.DataDirectory)
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Println("wal files has been removed")
	}
}
