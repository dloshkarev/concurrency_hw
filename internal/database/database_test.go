//go:build unit

package database_test

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/creator"
	"concurrency_hw/internal/database/network"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

func TestDatabase_Execute(t *testing.T) {
	cleanupDataDirs()
	logger, _ := zap.NewDevelopment()
	conf := config.Load()

	initializer := creator.NewCreator(logger, conf)

	walInstance, err := initializer.CreateWal()
	require.NoError(t, err)

	db, err := initializer.CreateDatabase(logger, conf.ReplicationConfig, walInstance)
	require.NoError(t, err)

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
				_, _ = db.Execute([]byte("SET key2 value2"))
			},
		},
		{
			name:  "DEL command success",
			query: "DEL key3",
			want:  network.SuccessCommand,
			setupFunc: func() {
				_, _ = db.Execute([]byte("SET key3 value3"))
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

			got, err := db.Execute([]byte(tt.query))

			if (err != nil) != tt.wantErr {
				t.Errorf("Database.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if string(got) != tt.want {
				t.Errorf("Database.Execute() = %v, want %v", got, tt.want)
			}

			if tt.cleanupFunc != nil {
				tt.cleanupFunc()
			}
		})
	}

	t.Run("Check WAL state", func(t *testing.T) {
		expected := map[int]string{
			0: "SET key1 value1",
			1: "SET key2 value2",
			2: "SET key3 value3",
			3: "DEL key3",
		}

		var idx int

		// Проверяем, что в WAL сохранены все запросы
		_ = walInstance.ForEach(func(queryString string) error {
			assert.Equal(t, expected[idx], queryString)
			idx++
			return nil
		})

		// Останавливаем БД и запускаем снова - предыдущий wal должен быть прочитан
		err = db.Stop()
		if err != nil {
			t.Fatal(err)
		}

		walInstance, err := initializer.CreateWal()
		require.NoError(t, err)

		db2, err := initializer.CreateDatabase(logger, conf.ReplicationConfig, walInstance)
		if err != nil {
			t.Fatal(err)
		}

		res, err := db2.Execute([]byte("GET key1"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, "value1"), string(res))

		res, err = db2.Execute([]byte("GET key2"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, "value2"), string(res))

		res, err = db2.Execute([]byte("GET key3"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(res))

		err = db2.Stop()
		if err != nil {
			t.Fatal(err)
		}
		cleanupDataDirs()
	})

	t.Run("Check WAL closing", func(t *testing.T) {
		cleanupDataDirs()

		// Чтобы запрос сразу не попал в файл
		conf.WalConfig.FlushingBatchSize = 100
		conf.WalConfig.FlushingBatchTimeout = 100 * time.Minute

		initializer2 := creator.NewCreator(logger, conf)

		walInstance, err := initializer2.CreateWal()
		require.NoError(t, err)

		db2, err := initializer2.CreateDatabase(logger, conf.ReplicationConfig, walInstance)
		if err != nil {
			t.Fatal(err)
		}

		// Проверяем что ключа нет
		res, err := db2.Execute([]byte("GET key1"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(res))

		res, err = db2.Execute([]byte("SET key1 value1"))
		require.NoError(t, err)
		assert.Equal(t, network.SuccessCommand, string(res))

		// Останавливаем БД - WAL должен записаться на диск
		err = db2.Stop()
		if err != nil {
			t.Fatal(err)
		}

		// Создаем новый WAL чтобы проверить что записалось на диск
		tmpWal, err := initializer.CreateWal()
		require.NoError(t, err)

		// Проверяем, что в WAL сохранены все запросы
		_ = tmpWal.ForEach(func(queryString string) error {
			assert.Equal(t, "SET key1 value1", queryString)
			return nil
		})

	})
}

func cleanupDataDirs() {
	dataDirs := []string{
		"/tmp/master-data-test",
		"/tmp/slave-data-test",
	}

	for _, dir := range dataDirs {
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("Warning: failed to remove %s: %v", dir, err)
		}
	}
}
