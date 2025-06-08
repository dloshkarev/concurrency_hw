package replicator

import (
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/wal"
)

type SlaveReplicator interface {
	GetUpdates() ([]string, error)
	Close() error
}

type TCPSlaveReplicator struct {
	tcpClient *network.TCPClient
	wal       wal.Wal
}

func NewTCPSlaveReplicator(tcpClient *network.TCPClient, wal wal.Wal) *TCPSlaveReplicator {
	return &TCPSlaveReplicator{
		tcpClient: tcpClient,
		wal:       wal,
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
		return nil, err
	}

	var response Response
	if err := Decode(tcpResponse, &response); err != nil {
		return nil, err
	}

	return response.Queries, nil
}

func (r *TCPSlaveReplicator) Close() error {
	err := r.tcpClient.Disconnect()
	if err != nil {
		return err
	}

	return nil
}
