//go:build e2e

package e2e

import (
	"concurrency_hw/internal/database/network"
	"fmt"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationSimple(t *testing.T) {
	// Очистка данных перед тестом
	cleanupDataDirs()

	// Запуск master сервера
	masterConfigPath, err := filepath.Abs("../config/config-master-test.yaml")
	require.NoError(t, err)

	masterCmd := exec.Command("../server", "-config", masterConfigPath)
	require.NoError(t, masterCmd.Start())
	defer func() {
		if masterCmd.Process != nil {
			_ = masterCmd.Process.Signal(syscall.SIGTERM)
			time.Sleep(time.Second)
		}
	}()

	// Ждем запуска master
	time.Sleep(3 * time.Second)

	// Запуск slave сервера
	slaveConfigPath, err := filepath.Abs("../config/config-slave-test.yaml")
	require.NoError(t, err)

	slaveCmd := exec.Command("../server", "-config", slaveConfigPath)
	require.NoError(t, slaveCmd.Start())
	defer func() {
		if slaveCmd.Process != nil {
			_ = slaveCmd.Process.Signal(syscall.SIGTERM)
			time.Sleep(time.Second)
		}
	}()

	// Ждем запуска slave
	time.Sleep(3 * time.Second)

	t.Run("BasicFunctionality", func(t *testing.T) {
		// Подключаемся к master
		masterClient, err := network.NewTCPClient("127.0.0.1:3223")
		require.NoError(t, err)
		defer func() {
			_ = masterClient.Disconnect()
		}()

		// SET на master должен работать
		response, err := masterClient.Execute([]byte("SET test_key test_value"))
		require.NoError(t, err)
		assert.Equal(t, network.SuccessCommand, string(response))

		// GET на master должен работать
		response, err = masterClient.Execute([]byte("GET test_key"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, "test_value"), string(response))

		// Ждем репликации
		time.Sleep(5 * time.Second)

		// Подключаемся к slave
		slaveClient, err := network.NewTCPClient("127.0.0.1:3224")
		require.NoError(t, err)
		defer func() {
			_ = slaveClient.Disconnect()
		}()

		// GET на slave должен работать и возвращать реплицированное значение
		response, err = slaveClient.Execute([]byte("GET test_key"))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, "test_value"), string(response))
	})

	t.Run("SlaveRejectsModifyingOperations", func(t *testing.T) {
		// Подключаемся к slave
		slaveClient, err := network.NewTCPClient("127.0.0.1:3224")
		require.NoError(t, err)
		defer func() {
			_ = slaveClient.Disconnect()
		}()

		// SET на slave должен возвращать ошибку
		response, err := slaveClient.Execute([]byte("SET slave_key slave_value"))
		require.NoError(t, err)
		assert.Contains(t, string(response), "cannot be executed on slave")
	})
}
