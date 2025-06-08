package database

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/network/replicator"
	"concurrency_hw/internal/database/storage/engine"
	"concurrency_hw/internal/database/storage/wal"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type PreProcessor interface {
	CleanQuery(queryString string) string
	ParseQuery(queryString string) (compute.Query, error)
}

type Database struct {
	logger            *zap.Logger
	preProcessor      PreProcessor
	engine            engine.Engine
	wal               wal.Wal
	replicationConfig *config.ReplicationConfig
	ticker            *time.Ticker
	replicator        replicator.SlaveReplicator
}

func NewDatabase(
	logger *zap.Logger,
	preProcessor PreProcessor,
	engine engine.Engine,
	wal wal.Wal,
	replicationConfig *config.ReplicationConfig,
	replicator replicator.SlaveReplicator,
) (*Database, error) {
	db := &Database{
		logger:            logger,
		preProcessor:      preProcessor,
		engine:            engine,
		wal:               wal,
		replicationConfig: replicationConfig,
		ticker:            time.NewTicker(replicationConfig.SyncInterval),
		replicator:        replicator,
	}

	err := db.Load()
	if err != nil {
		return nil, err
	}

	if replicationConfig.ReplicaType == config.Slave {
		err := db.runMasterSync()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (d *Database) Load() error {
	err := d.wal.ForEach(func(queryString string) error {
		_, err := d.executeWithWal(queryString, false)
		return err
	})

	if err != nil {
		return err
	}

	return nil
}

func (d *Database) Execute(query []byte) ([]byte, error) {
	queryString := string(query)
	response, err := d.executeWithWal(queryString, true)

	return []byte(response), err
}

func (d *Database) executeWithWal(queryString string, useWal bool) (string, error) {
	cleaned := d.preProcessor.CleanQuery(queryString)

	query, err := d.preProcessor.ParseQuery(cleaned)
	if err != nil {
		return network.CannotParseQuery, err
	}

	if _, exists := wal.WalCommands[query.CommandId]; exists &&
		d.replicationConfig.ReplicaType == config.Slave {
		return fmt.Sprintf(network.CommandReplicationError, query), err
	}

	if useWal {
		if _, exists := wal.WalCommands[query.CommandId]; exists {
			err = d.wal.Append(cleaned)
			if err != nil {
				return fmt.Sprintf(network.CommandStoreError, query), err
			}
		}
	}

	args := query.Args

	switch query.CommandId {
	case compute.SetCommandId:
		d.engine.Set(args[0], args[1])
		return network.SuccessCommand, nil
	case compute.GetCommandId:
		return fmt.Sprintf(network.GetResult, d.engine.Get(args[0])), nil
	case compute.DelCommandId:
		d.engine.Del(args[0])
		return network.SuccessCommand, nil
	default:
		return fmt.Sprintf(network.UnknownCommand, query.CommandId), err
	}
}

func (d *Database) runMasterSync() error {
	defer func() {
		err := d.replicator.Close()
		if err != nil {
			d.logger.Error("Failed to close master client", zap.Error(err))
		}
	}()

	go func() {
		for range d.ticker.C {
			queries, err := d.replicator.GetUpdates()
			if err != nil {
				panic(err)
			}

			if len(queries) > 0 {
				d.logger.Debug("run slave replication", zap.Int("queries", len(queries)))
				for _, queryString := range queries {
					_, err := d.executeWithWal(queryString, true)

					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	return nil
}

func (d *Database) Stop() error {
	d.ticker.Stop()

	err := d.wal.Close()
	if err != nil {
		return err
	}

	return nil
}
