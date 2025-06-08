package replicator

import (
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/wal"
	"strings"
)

type SlaveReplicator interface {
	GetUpdates() ([]string, error)
	Close() error
}

type TCPSlaveReplicator struct {
	tcpClient     *network.TCPClient
	wal           wal.Wal
	masterAddress string
}

func NewTCPSlaveReplicator(tcpClient *network.TCPClient, wal wal.Wal) *TCPSlaveReplicator {
	return &TCPSlaveReplicator{
		tcpClient:     tcpClient,
		wal:           wal,
		masterAddress: tcpClient.GetAddress(),
	}
}

func (r *TCPSlaveReplicator) GetUpdates() ([]string, error) {
	walStatus := r.wal.GetWalStatus()

	request, err := Encode(Request{
		LastSegmentNum:  walStatus.SegmentNum,
		LastSegmentLine: walStatus.SegmentLine,
	})
	if err != nil {
		return nil, err
	}

	tcpResponse, err := r.tcpClient.Execute(request)
	if err != nil {
		// Если ошибка связана с закрытым соединением, пытаемся переподключиться
		if r.isConnectionError(err) {
			if reconnectErr := r.reconnect(); reconnectErr != nil {
				return nil, reconnectErr
			}
			// Повторяем запрос после переподключения
			tcpResponse, err = r.tcpClient.Execute(request)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	var response Response
	if err := Decode(tcpResponse, &response); err != nil {
		return nil, err
	}

	return response.Queries, nil
}

func (r *TCPSlaveReplicator) isConnectionError(err error) bool {
	errorMsg := err.Error()
	return strings.Contains(errorMsg, "use of closed network connection") ||
		strings.Contains(errorMsg, "connection refused") ||
		strings.Contains(errorMsg, "EOF")
}

func (r *TCPSlaveReplicator) reconnect() error {
	// Закрываем старое соединение
	if r.tcpClient != nil {
		_ = r.tcpClient.Disconnect()
	}

	// Создаем новое соединение
	newClient, err := network.NewTCPClient(r.masterAddress)
	if err != nil {
		return err
	}

	r.tcpClient = newClient
	return nil
}

func (r *TCPSlaveReplicator) Close() error {
	return r.tcpClient.Disconnect()
}
