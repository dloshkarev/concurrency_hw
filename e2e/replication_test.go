//go:build e2e

package e2e

import (
	"concurrency_hw/internal/database/network"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	masterAddress      = "127.0.0.1:3223"
	slaveAddress       = "127.0.0.1:3224"
	replicationTimeout = 5 * time.Second
)

type TestServer struct {
	cmd        *exec.Cmd
	address    string
	configPath string
}

func TestReplication(t *testing.T) {
	// Очистка данных перед тестом
	cleanupDataDirs()

	// Запуск master сервера
	masterServer := startServer(t, "../config/config-master-test.yaml", masterAddress)
	defer stopServer(t, masterServer)

	// Ждем дополнительное время для полного запуска master и его репликатора
	time.Sleep(2 * time.Second)

	// Запуск slave сервера
	slaveServer := startServer(t, "../config/config-slave-test.yaml", slaveAddress)
	defer stopServer(t, slaveServer)

	t.Run("ModifyingOperationsOnlyOnMaster", func(t *testing.T) {
		// Создаем новых клиентов для каждого теста
		masterClient := getClientWithRetry(t, masterAddress)
		defer func() {
			_ = masterClient.Disconnect()
		}()

		slaveClient := getClientWithRetry(t, slaveAddress)
		defer func() {
			_ = slaveClient.Disconnect()
		}()

		testModifyingOperationsOnlyOnMaster(t, masterClient, slaveClient)
	})

	t.Run("ReadOperationsOnBothServers", func(t *testing.T) {
		// Создаем новых клиентов для каждого теста
		masterClient := getClientWithRetry(t, masterAddress)
		defer func() {
			_ = masterClient.Disconnect()
		}()

		slaveClient := getClientWithRetry(t, slaveAddress)
		defer func() {
			_ = slaveClient.Disconnect()
		}()

		testReadOperationsOnBothServers(t, masterClient, slaveClient)
	})

	t.Run("ReplicationFromMasterToSlave", func(t *testing.T) {
		// Создаем новых клиентов для каждого теста
		masterClient := getClientWithRetry(t, masterAddress)
		defer func() {
			_ = masterClient.Disconnect()
		}()

		slaveClient := getClientWithRetry(t, slaveAddress)
		defer func() {
			_ = slaveClient.Disconnect()
		}()

		testReplicationFromMasterToSlave(t, masterClient, slaveClient)
	})

	t.Run("ServerRestartAndWALRecovery", func(t *testing.T) {
		testServerRestartAndWALRecovery(t, masterServer, slaveServer)
	})
}

func testModifyingOperationsOnlyOnMaster(t *testing.T, masterClient, slaveClient *network.TCPClient) {
	// SET на master должен работать
	response, err := masterClient.Execute([]byte("SET test_key test_value"))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	// SET на slave должен возвращать ошибку
	response, err = slaveClient.Execute([]byte("SET slave_key slave_value"))
	require.NoError(t, err)
	assert.Contains(t, string(response), "cannot be executed on slave")

	// DEL на master должен работать
	response, err = masterClient.Execute([]byte("DEL test_key"))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	// DEL на slave должен возвращать ошибку
	response, err = slaveClient.Execute([]byte("DEL some_key"))
	require.NoError(t, err)
	assert.Contains(t, string(response), "cannot be executed on slave")
}

func testReadOperationsOnBothServers(t *testing.T, masterClient, slaveClient *network.TCPClient) {
	// SET на master
	response, err := masterClient.Execute([]byte("SET read_key read_value"))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	// Ждем репликации
	time.Sleep(replicationTimeout)

	// GET на master должен работать
	response, err = masterClient.Execute([]byte("GET read_key"))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, "read_value"), string(response))

	// GET на slave должен работать и возвращать то же значение
	response, err = slaveClient.Execute([]byte("GET read_key"))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, "read_value"), string(response))

	// GET несуществующего ключа на обоих серверах
	response, err = masterClient.Execute([]byte("GET nonexistent"))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(response))

	response, err = slaveClient.Execute([]byte("GET nonexistent"))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(response))
}

func testReplicationFromMasterToSlave(t *testing.T, masterClient, slaveClient *network.TCPClient) {
	// Выполним несколько операций на master
	operations := []string{
		"SET repl_key1 value1",
		"SET repl_key2 value2",
		"SET repl_key3 value3",
		"DEL repl_key2",
		"SET repl_key4 value4",
	}

	for _, op := range operations {
		response, err := masterClient.Execute([]byte(op))
		require.NoError(t, err)
		assert.Equal(t, network.SuccessCommand, string(response))
	}

	// Ждем репликации
	time.Sleep(replicationTimeout)

	// Проверяем, что все изменения реплицировались
	expectedValues := map[string]string{
		"repl_key1": "value1",
		"repl_key2": "", // был удален
		"repl_key3": "value3",
		"repl_key4": "value4",
	}

	for key, expectedValue := range expectedValues {
		response, err := slaveClient.Execute([]byte(fmt.Sprintf("GET %s", key)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, expectedValue), string(response))
	}
}

func testServerRestartAndWALRecovery(t *testing.T, masterServer, slaveServer *TestServer) {
	// Создаем новых клиентов
	masterClient := getClientWithRetry(t, masterAddress)
	defer func() {
		_ = masterClient.Disconnect()
	}()

	// Выполняем операции на master
	operations := []string{
		"SET restart_key1 restart_value1",
		"SET restart_key2 restart_value2",
		"SET restart_key3 restart_value3",
	}

	for _, op := range operations {
		response, err := masterClient.Execute([]byte(op))
		require.NoError(t, err)
		assert.Equal(t, network.SuccessCommand, string(response))
	}

	// Ждем репликации
	time.Sleep(replicationTimeout)

	// Останавливаем и перезапускаем master
	stopServer(t, masterServer)
	time.Sleep(time.Second)

	newMasterServer := startServer(t, masterServer.configPath, masterServer.address)
	defer stopServer(t, newMasterServer)

	// Останавливаем и перезапускаем slave
	stopServer(t, slaveServer)
	time.Sleep(time.Second)

	newSlaveServer := startServer(t, slaveServer.configPath, slaveServer.address)
	defer stopServer(t, newSlaveServer)

	// Создаем новых клиентов после перезапуска
	newMasterClient := getClientWithRetry(t, masterAddress)
	defer func() {
		_ = newMasterClient.Disconnect()
	}()

	newSlaveClient := getClientWithRetry(t, slaveAddress)
	defer func() {
		_ = newSlaveClient.Disconnect()
	}()

	// Ждем восстановления из WAL
	time.Sleep(replicationTimeout)

	// Проверяем, что данные восстановились на обоих серверах
	expectedValues := map[string]string{
		"restart_key1": "restart_value1",
		"restart_key2": "restart_value2",
		"restart_key3": "restart_value3",
	}

	for key, expectedValue := range expectedValues {
		// Проверяем на master
		response, err := newMasterClient.Execute([]byte(fmt.Sprintf("GET %s", key)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, expectedValue), string(response))

		// Проверяем на slave
		response, err = newSlaveClient.Execute([]byte(fmt.Sprintf("GET %s", key)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(network.GetResult, expectedValue), string(response))
	}
}

func startServer(t *testing.T, configPath, address string) *TestServer {
	// Получаем абсолютный путь к конфигу
	absConfigPath, err := filepath.Abs(configPath)
	require.NoError(t, err)

	cmd := exec.Command("../server", "-config", absConfigPath)

	// Добавляем логирование для отладки
	t.Logf("Starting server with config: %s", absConfigPath)

	require.NoError(t, cmd.Start())

	// Проверяем, что процесс запущен
	if cmd.Process == nil {
		t.Fatal("Server process not started")
	}

	// Ждем запуска сервера и проверяем, что он действительно слушает на порту
	serverStarted := false
	for i := 0; i < 20; i++ { // Максимум 10 секунд ожидания
		time.Sleep(500 * time.Millisecond)

		// Проверяем, что процесс еще жив
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("Server process exited with error: %v", cmd.ProcessState)
		}

		// Пытаемся подключиться к серверу
		if testConnection(address) {
			serverStarted = true
			t.Logf("Server successfully started on %s", address)
			break
		}
	}

	if !serverStarted {
		t.Fatalf("Server failed to start on %s within timeout", address)
	}

	return &TestServer{
		cmd:        cmd,
		address:    address,
		configPath: configPath,
	}
}

func testConnection(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func stopServer(t *testing.T, server *TestServer) {
	if server.cmd != nil && server.cmd.Process != nil {
		require.NoError(t, server.cmd.Process.Signal(syscall.SIGTERM))
		time.Sleep(time.Second)
	}
}

func getClientWithRetry(t *testing.T, address string) *network.TCPClient {
	var client *network.TCPClient
	var err error

	// Пытаемся подключиться с повторными попытками
	for i := 0; i < 10; i++ {
		t.Logf("Attempting to connect to %s (attempt %d/10)", address, i+1)
		client, err = network.NewTCPClient(address)
		if err == nil {
			t.Logf("Successfully connected to %s", address)
			return client
		}
		t.Logf("Connection failed: %v", err)
		time.Sleep(500 * time.Millisecond)
	}

	require.NoError(t, err, "Failed to connect to server after retries")
	return client
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
