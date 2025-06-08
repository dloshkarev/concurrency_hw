//go:build integration

package replicator_test

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/creator"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/network/replicator"
	"concurrency_hw/internal/database/storage/wal"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isConnectionClosed проверяет, является ли ошибка результатом закрытого соединения
func isConnectionClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

// MockWAL - заглушка для интерфейса WAL
type MockWAL struct {
	mu            sync.RWMutex
	queries       []string
	segmentNum    int
	segmentLine   int
	mockUpdates   []string
	getUpdatesErr error
}

func NewMockWAL() *MockWAL {
	return &MockWAL{
		queries:     make([]string, 0),
		segmentNum:  0,
		segmentLine: 0,
		mockUpdates: make([]string, 0),
	}
}

func (m *MockWAL) ForEach(f func(string) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, query := range m.queries {
		if err := f(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockWAL) Append(queryString string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queries = append(m.queries, queryString)
	m.segmentLine++
	return nil
}

func (m *MockWAL) GetUpdates(status *wal.WalStatus) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.getUpdatesErr != nil {
		return nil, m.getUpdatesErr
	}

	// Возвращаем обновления начиная с указанной позиции
	if status.SegmentNum > m.segmentNum || status.SegmentLine >= len(m.queries) {
		return []string{}, nil
	}

	startIndex := status.SegmentLine
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex >= len(m.queries) {
		return []string{}, nil
	}

	return m.queries[startIndex:], nil
}

func (m *MockWAL) GetWalStatus() *wal.WalStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &wal.WalStatus{
		SegmentNum:  m.segmentNum,
		SegmentLine: m.segmentLine,
	}
}

func (m *MockWAL) Close() error {
	return nil
}

// Дополнительные методы для тестирования
func (m *MockWAL) SetMockUpdates(updates []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mockUpdates = updates
}

func (m *MockWAL) SetGetUpdatesError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getUpdatesErr = err
}

func (m *MockWAL) AddQueries(queries ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, query := range queries {
		m.queries = append(m.queries, query)
		m.segmentLine++
	}
}

func (m *MockWAL) GetQueries() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, len(m.queries))
	copy(result, m.queries)
	return result
}

func TestReplicatorIntegration(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Настройка конфигурации
	masterConfig := &config.AppConfig{
		NetworkConfig: &config.NetworkConfig{
			Address:        "127.0.0.1:0", // Динамический порт
			MaxConnections: 10,
			MaxMessageSize: "4KB",
			IdleTimeout:    5 * time.Second,
		},
		ReplicationConfig: &config.ReplicationConfig{
			ReplicaType:   config.Master,
			MasterAddress: "127.0.0.1:0", // Будет обновлен позже
			SyncInterval:  1 * time.Second,
		},
	}

	// Создаем WAL для мастера с тестовыми данными
	masterWAL := NewMockWAL()
	masterWAL.AddQueries(
		"SET key1 value1",
		"SET key2 value2",
		"DEL key3",
	)

	// Создаем creator для мастера
	masterCreator := creator.NewCreator(logger, masterConfig)

	// Получаем свободный порт для мастера
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	masterAddress := listener.Addr().String()
	require.NoError(t, listener.Close())

	// Обновляем конфигурацию с реальным адресом
	masterConfig.ReplicationConfig.MasterAddress = masterAddress
	masterConfig.NetworkConfig.Address = masterAddress

	// Создаем TCP сервер для мастера
	masterServer, err := masterCreator.CreateMasterReplicatorServer(logger, masterConfig, masterWAL)
	require.NoError(t, err)

	// Запускаем мастер сервер в горутине
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverError error
	serverWg := sync.WaitGroup{}
	serverWg.Add(1)

	go func() {
		defer serverWg.Done()
		serverError = masterServer.Run(ctx)
	}()

	// Ждем запуска сервера
	time.Sleep(100 * time.Millisecond)

	t.Run("Successful replication", func(t *testing.T) {
		// Создаем TCP клиент для slave
		slaveClient, err := network.NewTCPClient(masterAddress)
		require.NoError(t, err)
		defer func() {
			if err := slaveClient.Disconnect(); err != nil && !isConnectionClosed(err) {
				require.NoError(t, err)
			}
		}()

		// Создаем WAL для slave
		slaveWAL := NewMockWAL()
		slaveWAL.segmentNum = 0
		slaveWAL.segmentLine = 0 // Начинаем с самого начала

		// Создаем slave replicator
		slaveReplicator := replicator.NewTCPSlaveReplicator(slaveClient, slaveWAL)

		// Получаем обновления
		updates, err := slaveReplicator.GetUpdates()
		require.NoError(t, err)

		// Проверяем, что получили все ожидаемые обновления
		expectedUpdates := []string{
			"SET key1 value1",
			"SET key2 value2",
			"DEL key3",
		}

		assert.Equal(t, expectedUpdates, updates)

		// Закрываем replicator
		err = slaveReplicator.Close()
		assert.NoError(t, err)
	})

	t.Run("Partial replication from specific position", func(t *testing.T) {
		// Создаем TCP клиент для slave
		slaveClient, err := network.NewTCPClient(masterAddress)
		require.NoError(t, err)
		defer func() {
			if err := slaveClient.Disconnect(); err != nil && !isConnectionClosed(err) {
				require.NoError(t, err)
			}
		}()

		// Создаем WAL для slave с уже имеющимися данными
		slaveWAL := NewMockWAL()
		slaveWAL.segmentNum = 0
		slaveWAL.segmentLine = 1 // Уже есть первая запись

		// Создаем slave replicator
		slaveReplicator := replicator.NewTCPSlaveReplicator(slaveClient, slaveWAL)

		// Получаем обновления начиная с позиции 1
		updates, err := slaveReplicator.GetUpdates()
		require.NoError(t, err)

		// Должны получить только записи начиная с позиции 1
		expectedUpdates := []string{
			"SET key2 value2",
			"DEL key3",
		}

		assert.Equal(t, expectedUpdates, updates)

		err = slaveReplicator.Close()
		assert.NoError(t, err)
	})

	t.Run("No updates when slave is up to date", func(t *testing.T) {
		// Создаем TCP клиент для slave
		slaveClient, err := network.NewTCPClient(masterAddress)
		require.NoError(t, err)
		defer func() {
			if err := slaveClient.Disconnect(); err != nil && !isConnectionClosed(err) {
				require.NoError(t, err)
			}
		}()

		// Создаем WAL для slave который полностью синхронизирован
		slaveWAL := NewMockWAL()
		slaveWAL.segmentNum = 0
		slaveWAL.segmentLine = 3 // Все записи уже есть

		// Создаем slave replicator
		slaveReplicator := replicator.NewTCPSlaveReplicator(slaveClient, slaveWAL)

		// Получаем обновления
		updates, err := slaveReplicator.GetUpdates()
		require.NoError(t, err)

		// Не должны получить никаких обновлений
		assert.Empty(t, updates)

		err = slaveReplicator.Close()
		assert.NoError(t, err)
	})

	t.Run("Multiple clients can connect simultaneously", func(t *testing.T) {
		const clientCount = 3
		var wg sync.WaitGroup
		var clientErrors []error
		clientUpdatesMap := make(map[int][]string)
		var mu sync.Mutex

		for i := 0; i < clientCount; i++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()

				// Создаем TCP клиент для slave
				slaveClient, err := network.NewTCPClient(masterAddress)
				if err != nil {
					mu.Lock()
					clientErrors = append(clientErrors, err)
					mu.Unlock()
					return
				}
				defer func() {
					if err := slaveClient.Disconnect(); err != nil && !isConnectionClosed(err) {
						mu.Lock()
						clientErrors = append(clientErrors, err)
						mu.Unlock()
					}
				}()

				// Создаем WAL для slave
				slaveWAL := NewMockWAL()
				slaveWAL.segmentNum = 0
				slaveWAL.segmentLine = clientID // Разные позиции для разных клиентов

				// Создаем slave replicator
				slaveReplicator := replicator.NewTCPSlaveReplicator(slaveClient, slaveWAL)

				// Получаем обновления
				updates, err := slaveReplicator.GetUpdates()
				mu.Lock()
				if err != nil {
					clientErrors = append(clientErrors, err)
				} else {
					clientUpdatesMap[clientID] = updates
				}
				mu.Unlock()

				if err := slaveReplicator.Close(); err != nil {
					mu.Lock()
					clientErrors = append(clientErrors, err)
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// Проверяем, что не было ошибок
		assert.Empty(t, clientErrors, "Expected no client errors")

		// Проверяем, что все клиенты получили ответы
		assert.Len(t, clientUpdatesMap, clientCount)

		// Проверяем специфичные обновления для каждого клиента
		expectedForClient0 := []string{"SET key1 value1", "SET key2 value2", "DEL key3"}
		expectedForClient1 := []string{"SET key2 value2", "DEL key3"}
		expectedForClient2 := []string{"DEL key3"}

		assert.Equal(t, expectedForClient0, clientUpdatesMap[0])
		assert.Equal(t, expectedForClient1, clientUpdatesMap[1])
		assert.Equal(t, expectedForClient2, clientUpdatesMap[2])
	})

	t.Run("Master rejects slave configuration", func(t *testing.T) {
		// Создаем mock request для тестирования
		mockRequest, err := replicator.Encode(replicator.Request{
			LastSegmentNum:  0,
			LastSegmentLine: 0,
		})
		require.NoError(t, err)

		// Создаем конфигурацию с неправильным типом реплики
		wrongConfig := &config.ReplicationConfig{
			ReplicaType: config.Slave, // Slave пытается стать мастером
		}

		wrongWAL := NewMockWAL()

		// Создаем master replicator напрямую для тестирования ошибки
		masterReplicator := replicator.NewTCPMasterReplicator(wrongConfig, wrongWAL)

		// Пытаемся получить обновления - должно вернуть ошибку
		response, err := masterReplicator.GetSegmentUpdates(mockRequest)
		assert.Error(t, err)
		assert.Contains(t, string(response), "slave cannot handle replication request")
	})

	// Останавливаем мастер сервер
	cancel()

	// Ждем завершения сервера с таймаутом
	done := make(chan struct{})
	go func() {
		serverWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.NoError(t, serverError)
	case <-time.After(2 * time.Second):
		t.Log("Server shutdown timed out, but test completed successfully")
	}
}

func TestTCPSlaveReplicator_UnitTests(t *testing.T) {
	t.Run("NewTCPSlaveReplicator creation", func(t *testing.T) {
		// Создаем mock компоненты
		mockClient := &network.TCPClient{} // Поскольку поле приватное, создаем пустую структуру
		mockWAL := NewMockWAL()

		// Создаем rep
		rep := replicator.NewTCPSlaveReplicator(mockClient, mockWAL)

		// Проверяем, что rep создался
		assert.NotNil(t, rep)
	})

	t.Run("GetWalStatus from mock WAL", func(t *testing.T) {
		mockWAL := NewMockWAL()
		mockWAL.segmentNum = 5
		mockWAL.segmentLine = 10

		status := mockWAL.GetWalStatus()

		assert.Equal(t, 5, status.SegmentNum)
		assert.Equal(t, 10, status.SegmentLine)
	})

	t.Run("MockWAL ForEach functionality", func(t *testing.T) {
		mockWAL := NewMockWAL()
		mockWAL.AddQueries("query1", "query2", "query3")

		var collected []string
		err := mockWAL.ForEach(func(query string) error {
			collected = append(collected, query)
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, []string{"query1", "query2", "query3"}, collected)
	})

	t.Run("MockWAL GetUpdates with different positions", func(t *testing.T) {
		mockWAL := NewMockWAL()
		mockWAL.AddQueries("query1", "query2", "query3", "query4")

		// Тест получения обновлений с позиции 0
		updates, err := mockWAL.GetUpdates(&wal.WalStatus{SegmentNum: 0, SegmentLine: 0})
		assert.NoError(t, err)
		assert.Equal(t, []string{"query1", "query2", "query3", "query4"}, updates)

		// Тест получения обновлений с позиции 2
		updates, err = mockWAL.GetUpdates(&wal.WalStatus{SegmentNum: 0, SegmentLine: 2})
		assert.NoError(t, err)
		assert.Equal(t, []string{"query3", "query4"}, updates)

		// Тест получения обновлений с позиции за границей
		updates, err = mockWAL.GetUpdates(&wal.WalStatus{SegmentNum: 0, SegmentLine: 10})
		assert.NoError(t, err)
		assert.Empty(t, updates)
	})
}

func TestProtocolEncodeDecode(t *testing.T) {
	t.Run("Request encoding and decoding", func(t *testing.T) {
		originalRequest := replicator.Request{
			LastSegmentNum:  5,
			LastSegmentLine: 10,
		}

		// Кодируем
		encoded, err := replicator.Encode(originalRequest)
		require.NoError(t, err)
		assert.NotEmpty(t, encoded)

		// Декодируем
		var decodedRequest replicator.Request
		err = replicator.Decode(encoded, &decodedRequest)
		require.NoError(t, err)

		// Проверяем, что данные совпадают
		assert.Equal(t, originalRequest.LastSegmentNum, decodedRequest.LastSegmentNum)
		assert.Equal(t, originalRequest.LastSegmentLine, decodedRequest.LastSegmentLine)
	})

	t.Run("Response encoding and decoding", func(t *testing.T) {
		originalResponse := replicator.Response{
			Queries: []string{"SET key1 value1", "GET key2", "DEL key3"},
		}

		// Кодируем
		encoded, err := replicator.Encode(originalResponse)
		require.NoError(t, err)
		assert.NotEmpty(t, encoded)

		// Декодируем
		var decodedResponse replicator.Response
		err = replicator.Decode(encoded, &decodedResponse)
		require.NoError(t, err)

		// Проверяем, что данные совпадают
		assert.Equal(t, originalResponse.Queries, decodedResponse.Queries)
	})

	t.Run("Empty Response encoding and decoding", func(t *testing.T) {
		originalResponse := replicator.Response{
			Queries: []string{},
		}

		// Кодируем
		encoded, err := replicator.Encode(originalResponse)
		require.NoError(t, err)
		assert.NotEmpty(t, encoded)

		// Декодируем
		var decodedResponse replicator.Response
		err = replicator.Decode(encoded, &decodedResponse)
		require.NoError(t, err)

		// Проверяем, что данные пустые (может быть nil или пустой slice)
		assert.Empty(t, decodedResponse.Queries)
		assert.True(t, len(decodedResponse.Queries) == 0)
	})

	t.Run("Invalid data decoding", func(t *testing.T) {
		invalidData := []byte("invalid gob data")

		var request replicator.Request
		err := replicator.Decode(invalidData, &request)
		assert.Error(t, err)

		var response replicator.Response
		err = replicator.Decode(invalidData, &response)
		assert.Error(t, err)
	})
}

func TestTCPMasterReplicator_UnitTests(t *testing.T) {
	t.Run("Master replicator creation", func(t *testing.T) {
		conf := &config.ReplicationConfig{
			ReplicaType: config.Master,
		}
		mockWAL := NewMockWAL()

		masterReplicator := replicator.NewTCPMasterReplicator(conf, mockWAL)

		assert.NotNil(t, masterReplicator)
	})

	t.Run("GetSegmentUpdates with valid request", func(t *testing.T) {
		conf := &config.ReplicationConfig{
			ReplicaType: config.Master,
		}
		mockWAL := NewMockWAL()
		mockWAL.AddQueries("SET key1 value1", "SET key2 value2")

		masterReplicator := replicator.NewTCPMasterReplicator(conf, mockWAL)

		// Создаем валидный запрос
		request := replicator.Request{
			LastSegmentNum:  0,
			LastSegmentLine: 0,
		}
		requestBytes, err := replicator.Encode(request)
		require.NoError(t, err)

		// Получаем обновления
		responseBytes, err := masterReplicator.GetSegmentUpdates(requestBytes)
		require.NoError(t, err)

		// Декодируем ответ
		var response replicator.Response
		err = replicator.Decode(responseBytes, &response)
		require.NoError(t, err)

		// Проверяем содержимое
		expectedQueries := []string{"SET key1 value1", "SET key2 value2"}
		assert.Equal(t, expectedQueries, response.Queries)
	})

	t.Run("GetSegmentUpdates with slave configuration error", func(t *testing.T) {
		conf := &config.ReplicationConfig{
			ReplicaType: config.Slave, // Неправильная конфигурация
		}
		mockWAL := NewMockWAL()

		masterReplicator := replicator.NewTCPMasterReplicator(conf, mockWAL)

		// Создаем запрос
		request := replicator.Request{
			LastSegmentNum:  0,
			LastSegmentLine: 0,
		}
		requestBytes, err := replicator.Encode(request)
		require.NoError(t, err)

		// Должна быть ошибка
		responseBytes, err := masterReplicator.GetSegmentUpdates(requestBytes)
		assert.Error(t, err)
		assert.Contains(t, string(responseBytes), "slave cannot handle replication request")
	})

	t.Run("GetSegmentUpdates with invalid request", func(t *testing.T) {
		conf := &config.ReplicationConfig{
			ReplicaType: config.Master,
		}
		mockWAL := NewMockWAL()

		masterReplicator := replicator.NewTCPMasterReplicator(conf, mockWAL)

		// Невалидные данные
		invalidRequestBytes := []byte("invalid request data")

		// Должна быть ошибка декодирования
		_, err := masterReplicator.GetSegmentUpdates(invalidRequestBytes)
		assert.Error(t, err)
	})
}
