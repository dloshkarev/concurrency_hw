package database

import (
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/engine"
	"fmt"
)

type PreProcessor interface {
	ParseQuery(queryString string) (compute.Query, error)
}

type Database struct {
	preProcessor PreProcessor
	engine       engine.Engine
}

func NewDatabase(
	preProcessor PreProcessor,
	engine engine.Engine,
) *Database {
	return &Database{
		preProcessor: preProcessor,
		engine:       engine,
	}
}

func (d *Database) Execute(queryString string) (string, error) {
	query, err := d.preProcessor.ParseQuery(queryString)
	if err != nil {
		return network.CannotParseQuery, err
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
