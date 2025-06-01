package database

import (
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/engine"
	"concurrency_hw/internal/database/storage/wal"
	"fmt"
)

type PreProcessor interface {
	ParseQuery(queryString string) (compute.Query, error)
}

type Database struct {
	preProcessor PreProcessor
	engine       engine.Engine
	wal          wal.Wal
}

func NewDatabase(
	preProcessor PreProcessor,
	engine engine.Engine,
	wal wal.Wal,
) (*Database, error) {
	db := &Database{
		preProcessor: preProcessor,
		engine:       engine,
		wal:          wal,
	}

	err := db.Load()
	if err != nil {
		return nil, err
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

func (d *Database) Execute(queryString string) (string, error) {
	return d.executeWithWal(queryString, true)
}

func (d *Database) executeWithWal(queryString string, useWal bool) (string, error) {
	query, err := d.preProcessor.ParseQuery(queryString)
	if err != nil {
		return network.CannotParseQuery, err
	}

	if useWal {
		if _, exists := wal.WalCommands[query.CommandId]; exists {
			err = d.wal.Append(queryString)
			if err != nil {
				return network.CommandStoreError, err
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

func (d *Database) Stop() error {
	err := d.wal.Close()
	if err != nil {
		return err
	}

	return nil
}
