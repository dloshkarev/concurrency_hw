package creator

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database"
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/network/replicator"
	"concurrency_hw/internal/database/storage/engine"
	"concurrency_hw/internal/database/storage/engine/mem"
	"concurrency_hw/internal/database/storage/engine/partition"
	"concurrency_hw/internal/database/storage/wal"
	"go.uber.org/zap"
)

type Creator struct {
	logger *zap.Logger
	conf   *config.AppConfig
}

func NewCreator(logger *zap.Logger, conf *config.AppConfig) *Creator {
	return &Creator{
		logger: logger,
		conf:   conf,
	}
}

func (c *Creator) CreateWal() (wal.Wal, error) {
	walReader, lastSegment, err := wal.NewStringSegmentReader(c.conf.WalConfig)
	if err != nil {
		return nil, err
	}

	walWriter, err := wal.NewStringSegmentWriter(c.conf.WalConfig, lastSegment)
	if err != nil {
		return nil, err
	}

	walInstance, err := wal.NewSegmentedFSWal(c.conf.WalConfig, c.logger, lastSegment, walReader, walWriter)
	if err != nil {
		return nil, err
	}

	return walInstance, nil
}

func (c *Creator) CreateDatabase(
	logger *zap.Logger,
	conf *config.ReplicationConfig,
	walInstance wal.Wal,
) (*database.Database, error) {
	parser, err := compute.NewQueryParser(c.logger)
	if err != nil {
		c.logger.Fatal("Failed to create query parser", zap.Error(err))
	}

	var eng engine.Engine
	if c.conf.EngineConfig.PartitionsCount > 0 {
		partitions := make([]engine.Engine, 0, c.conf.EngineConfig.PartitionsCount)
		for i := 0; i < c.conf.EngineConfig.PartitionsCount; i++ {
			partitions[i] = mem.NewInMemoryEngine(c.conf.EngineConfig.StartSize)
		}
		eng = partition.NewPartitionedEngine(partitions)
	} else {
		eng = mem.NewInMemoryEngine(c.conf.EngineConfig.StartSize)
	}

	var replicatorClient replicator.SlaveReplicator = nil
	if conf.ReplicaType == config.Slave {
		replicationTcpClient, err := network.NewTCPClient(conf.MasterAddress)
		if err != nil {
			return nil, err
		}

		replicatorClient = replicator.NewTCPSlaveReplicator(replicationTcpClient, walInstance)
	}

	return database.NewDatabase(logger, parser, eng, walInstance, conf, replicatorClient)
}

func (c *Creator) CreateMasterReplicatorServer(
	logger *zap.Logger,
	conf *config.AppConfig,
	walInstance wal.Wal,
) (*network.TCPServer, error) {
	master := replicator.NewTCPMasterReplicator(conf.ReplicationConfig, walInstance)

	masterReplicatorServer, err := c.CreateTCPServer(logger, conf.ReplicatorNetworkConfig(), master.GetSegmentUpdates)
	if err != nil {
		return nil, err
	}

	return masterReplicatorServer, nil
}

func (c *Creator) CreateTCPServer(
	logger *zap.Logger,
	conf *config.NetworkConfig,
	requestHandler func([]byte) ([]byte, error),
) (*network.TCPServer, error) {
	server, err := network.NewTCPServer(logger, conf, requestHandler)
	if err != nil {
		return nil, err
	}

	return server, nil
}
