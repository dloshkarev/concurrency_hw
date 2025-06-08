package partition

import (
	"concurrency_hw/internal/database/storage/engine"
	"hash/fnv"
)

type PartitionedEngine struct {
	partitions []engine.Engine
}

func NewPartitionedEngine(partitions []engine.Engine) *PartitionedEngine {
	return &PartitionedEngine{
		partitions: partitions,
	}
}

func (e *PartitionedEngine) Set(key, value string) {
	e.withPartition(key, func(eng engine.Engine) string {
		eng.Set(key, value)
		return ""
	})
}

func (e *PartitionedEngine) Get(key string) string {
	return e.withPartition(key, func(eng engine.Engine) string {
		return eng.Get(key)
	})
}

func (e *PartitionedEngine) Del(key string) {
	e.withPartition(key, func(eng engine.Engine) string {
		eng.Del(key)
		return ""
	})
}

func (e *PartitionedEngine) withPartition(key string, f func(engine.Engine) string) string {
	idx := e.getPartitionId(key)
	return f(e.partitions[idx])
}

func (e *PartitionedEngine) getPartitionId(key string) int {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		panic(err)
	}

	return int(hasher.Sum32()) % len(e.partitions)
}
