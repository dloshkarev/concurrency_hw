package creator

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database"
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/storage/engine/mem"
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

func (i *Creator) CreateWal() (wal.Wal, error) {
	walReader := wal.NewStringSegmentReader(i.conf.WalConfig)

	lastSegment, err := walReader.Open()
	if err != nil {
		return nil, err
	}

	walWriter, err := wal.NewStringSegmentWriter(i.conf.WalConfig, lastSegment)
	if err != nil {
		return nil, err
	}

	walInstance, err := wal.NewSegmentedFSWal(i.conf.WalConfig, walReader, walWriter)
	if err != nil {
		return nil, err
	}

	return walInstance, nil
}

func (i *Creator) CreateDatabase() *database.Database {
	parser, err := compute.NewQueryParser(i.logger)
	if err != nil {
		i.logger.Fatal("Failed to create query parser", zap.Error(err))
	}

	engine := mem.NewInMemoryEngine(i.conf.EngineConfig.StartSize)

	walInstance, err := i.CreateWal()
	if err != nil {
		i.logger.Fatal("Failed to create wal", zap.Error(err))
	}

	return database.NewDatabase(parser, engine, walInstance)
}
