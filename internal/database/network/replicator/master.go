package replicator

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/wal"
	"fmt"
)

type MasterReplicator interface {
	GetSegmentUpdates(requestBytes []byte) ([]byte, error)
}

type TCPMasterReplicator struct {
	conf *config.ReplicationConfig
	wal  wal.Wal
}

func NewTCPMasterReplicator(conf *config.ReplicationConfig, wal wal.Wal) *TCPMasterReplicator {
	return &TCPMasterReplicator{
		conf: conf,
		wal:  wal,
	}
}

func (r *TCPMasterReplicator) GetSegmentUpdates(requestBytes []byte) ([]byte, error) {
	if r.conf.ReplicaType == config.Slave {
		return []byte(network.SlaveReplicationError), fmt.Errorf("slave cannot handle replication request")
	}

	var request Request
	err := Decode(requestBytes, &request)
	if err != nil {
		return nil, err
	}

	queries, err := r.wal.GetUpdates(&wal.WalStatus{
		SegmentNum:  request.LastSegmentNum,
		SegmentLine: request.LastSegmentLine,
	})
	if err != nil {
		return nil, err
	}

	response, err := Encode(Response{
		Queries: queries,
	})
	if err != nil {
		return nil, err
	}

	return response, err
}
