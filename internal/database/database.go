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
) *Database {
	return &Database{
		preProcessor: preProcessor,
		engine:       engine,
		wal:          wal,
	}
}

func (d *Database) Load() error {
	err := d.wal.ForEach(func(queryString string) error {
		_, err := d.Execute(queryString)
		return err
	})

	if err != nil {
		return err
	}

	return nil
}

func (d *Database) Execute(queryString string) (string, error) {
	query, err := d.preProcessor.ParseQuery(queryString)
	if err != nil {
		return network.CannotParseQuery, err
	}

	err = d.wal.Append(queryString)
	if err != nil {
		return network.CommandStoreError, err
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
