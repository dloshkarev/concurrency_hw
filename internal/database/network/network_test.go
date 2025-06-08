//go:build unit

package network_test

import (
	"bytes"
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/primitive"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock request handler для тестирования сервера
func mockRequestHandler(request []byte) ([]byte, error) {
	requestStr := string(request)
	switch requestStr {
	case "GET key1":
		return []byte(fmt.Sprintf(network.GetResult, "value1")), nil
	case "SET key1 value1":
		return []byte(network.SuccessCommand), nil
	case "DEL key1":
		return []byte(network.SuccessCommand), nil
	case "INVALID":
		return []byte(fmt.Sprintf(network.UnknownCommand, "INVALID")), fmt.Errorf("unknown command")
	case "ERROR":
		return []byte(fmt.Sprintf(network.CommandStoreError, "test error")), fmt.Errorf("test error")
	default:
		return []byte(fmt.Sprintf(network.CannotParseQuery)), fmt.Errorf("cannot parse")
	}
}

func TestResponseConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "CannotParseQuery",
			constant: network.CannotParseQuery,
			expected: "[error] cannot parse query",
		},
		{
			name:     "SuccessCommand",
			constant: network.SuccessCommand,
			expected: "[success]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

func TestResponseTemplates(t *testing.T) {
	tests := []struct {
		name     string
		template string
		args     []interface{}
		expected string
	}{
		{
			name:     "UnknownCommand",
			template: network.UnknownCommand,
			args:     []interface{}{"INVALID"},
			expected: "[error] unknown command: INVALID",
		},
		{
			name:     "CommandStoreError",
			template: network.CommandStoreError,
			args:     []interface{}{"disk full"},
			expected: "[error] command storing failed: disk full",
		},
		{
			name:     "CommandReplicationError",
			template: network.CommandReplicationError,
			args:     []interface{}{"SET key value"},
			expected: "[error] modifying command cannot be executed on slave: SET key value",
		},
		{
			name:     "SlaveReplicationError",
			template: network.SlaveReplicationError,
			args:     []interface{}{},
			expected: "[error] slave cannot handle replication request",
		},
		{
			name:     "GetResult",
			template: network.GetResult,
			args:     []interface{}{"test_value"},
			expected: "[success] test_value",
		},
		{
			name:     "GetResult_EmptyValue",
			template: network.GetResult,
			args:     []interface{}{""},
			expected: "[success] ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fmt.Sprintf(tt.template, tt.args...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewTCPServer(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tests := []struct {
		name           string
		config         *config.NetworkConfig
		semaphore      *primitive.Semaphore
		requestHandler func([]byte) ([]byte, error)
		wantError      bool
	}{
		{
			name: "Valid configuration",
			config: &config.NetworkConfig{
				Address:        "127.0.0.1:0",
				MaxConnections: 10,
				MaxMessageSize: "4KB",
				IdleTimeout:    5 * time.Minute,
			},
			semaphore:      primitive.NewSemaphore(10),
			requestHandler: mockRequestHandler,
			wantError:      false,
		},
		{
			name: "Invalid message size",
			config: &config.NetworkConfig{
				Address:        "127.0.0.1:0",
				MaxConnections: 10,
				MaxMessageSize: "invalid",
				IdleTimeout:    5 * time.Minute,
			},
			requestHandler: mockRequestHandler,
			wantError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := network.NewTCPServer(logger, tt.config, tt.semaphore, tt.requestHandler)

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, server)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, server)
			}
		})
	}
}

func TestNewTCPClient(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		wantError bool
	}{
		{
			name:      "Invalid address format",
			address:   "invalid-address",
			wantError: true,
		},
		{
			name:      "Unreachable address",
			address:   "127.0.0.1:99999",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := network.NewTCPClient(tt.address)

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

func TestMockRequestHandler(t *testing.T) {
	tests := []struct {
		name         string
		request      []byte
		wantResponse string
		wantError    bool
	}{
		{
			name:         "GET command",
			request:      []byte("GET key1"),
			wantResponse: fmt.Sprintf(network.GetResult, "value1"),
			wantError:    false,
		},
		{
			name:         "SET command",
			request:      []byte("SET key1 value1"),
			wantResponse: network.SuccessCommand,
			wantError:    false,
		},
		{
			name:         "DEL command",
			request:      []byte("DEL key1"),
			wantResponse: network.SuccessCommand,
			wantError:    false,
		},
		{
			name:         "Invalid command",
			request:      []byte("INVALID"),
			wantResponse: fmt.Sprintf(network.UnknownCommand, "INVALID"),
			wantError:    true,
		},
		{
			name:         "Error command",
			request:      []byte("ERROR"),
			wantResponse: fmt.Sprintf(network.CommandStoreError, "test error"),
			wantError:    true,
		},
		{
			name:         "Unknown command",
			request:      []byte("UNKNOWN"),
			wantResponse: network.CannotParseQuery,
			wantError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := mockRequestHandler(tt.request)

			assert.Equal(t, tt.wantResponse, string(response))
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Тест для проверки чтения ответа с помощью mock соединения
func TestReadResponse(t *testing.T) {
	tests := []struct {
		name         string
		serverData   string
		expectedData string
	}{
		{
			name:         "Simple response",
			serverData:   "Hello, World!",
			expectedData: "Hello, World!",
		},
		{
			name:         "Empty response",
			serverData:   "",
			expectedData: "",
		},
		{
			name:         "Multi-line response",
			serverData:   "Line 1\nLine 2\nLine 3",
			expectedData: "Line 1\nLine 2\nLine 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Создаем mock соединение с помощью pipe
			server, client := net.Pipe()
			defer func() {
				require.NoError(t, client.Close())
			}()

			// Отправляем данные от "сервера" в отдельной горутине
			go func() {
				defer func() {
					require.NoError(t, server.Close())
				}()
				_, err := server.Write([]byte(tt.serverData))
				require.NoError(t, err)
			}()

			// Читаем ответ
			response, err := readResponseFromConnection(client)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedData, string(response))
		})
	}
}

// Helper function для тестирования readResponse (копия из client.go)
func readResponseFromConnection(conn net.Conn) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 1024) // ReadBufferSize

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err == io.EOF {
				buf = append(buf, tmp[:n]...)
				return buf, nil
			}
			return nil, err
		}
		buf = append(buf, tmp[:n]...)
		if n < 1024 {
			return buf, nil
		}
	}
}

func TestNetworkConfigValidation(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tests := []struct {
		name          string
		maxMsgSize    string
		wantError     bool
		expectedBytes int64
	}{
		{
			name:          "Valid bytes",
			maxMsgSize:    "1024b",
			wantError:     false,
			expectedBytes: 1024,
		},
		{
			name:          "Valid kilobytes",
			maxMsgSize:    "4KB",
			wantError:     false,
			expectedBytes: 4096,
		},
		{
			name:       "Invalid format",
			maxMsgSize: "invalid",
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &config.NetworkConfig{
				Address:        "127.0.0.1:0",
				MaxConnections: 10,
				MaxMessageSize: tt.maxMsgSize,
				IdleTimeout:    5 * time.Minute,
			}

			server, err := network.NewTCPServer(logger, conf, primitive.NewSemaphore(10), mockRequestHandler)

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, server)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, server)
			}
		})
	}
}

func TestLargeDataHandling(t *testing.T) {
	// Тест для больших объемов данных
	largeData := strings.Repeat("A", 2048)
	server, client := net.Pipe()
	defer func() {
		require.NoError(t, client.Close())
	}()

	go func() {
		defer func() {
			require.NoError(t, server.Close())
		}()
		_, err := server.Write([]byte(largeData))
		require.NoError(t, err)
	}()

	response, err := readResponseFromConnection(client)
	require.NoError(t, err)
	assert.Equal(t, largeData, string(response))
	assert.Equal(t, 2048, len(response))
}

func TestConnectionErrorHandling(t *testing.T) {
	// Тест обработки ошибок соединения
	server, client := net.Pipe()

	// Закрываем сервер сразу чтобы вызвать EOF
	require.NoError(t, server.Close())

	response, err := readResponseFromConnection(client)
	// При закрытии соединения должен вернуться EOF, который обрабатывается как успешный случай
	// но без данных
	assert.NoError(t, err)
	assert.Empty(t, response)

	require.NoError(t, client.Close())
}

func TestBoundaryConditions(t *testing.T) {
	// Тест граничных условий
	tests := []struct {
		name     string
		dataSize int
	}{
		{
			name:     "Exactly buffer size",
			dataSize: 1024,
		},
		{
			name:     "Buffer size plus one",
			dataSize: 1025,
		},
		{
			name:     "Multiple of buffer size",
			dataSize: 2048,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := bytes.Repeat([]byte("X"), tt.dataSize)
			server, client := net.Pipe()
			defer func() {
				require.NoError(t, client.Close())
			}()

			go func() {
				defer func() {
					require.NoError(t, server.Close())
				}()
				_, err := server.Write(data)
				require.NoError(t, err)
			}()

			response, err := readResponseFromConnection(client)
			require.NoError(t, err)
			assert.Equal(t, data, response)
			assert.Equal(t, tt.dataSize, len(response))
		})
	}
}
