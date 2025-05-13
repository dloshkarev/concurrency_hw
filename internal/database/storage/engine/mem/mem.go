package mem

import (
	"go.uber.org/zap"
)

// TODO: о потокобезопасности речи пока не идет
type InMemoryEngine struct {
	logger  *zap.Logger
	storage map[string]string
}

func NewEngine(logger *zap.Logger, initialSize int) *InMemoryEngine {
	logger.Info("In-memory engine has been initialized")
	return &InMemoryEngine{
		logger:  logger,
		storage: make(map[string]string, initialSize),
	}
}

func (e *InMemoryEngine) Set(key, value string) {
	e.storage[key] = value
}

func (e *InMemoryEngine) Get(key string) string {
	return e.storage[key]
}

func (e *InMemoryEngine) Del(key string) {
	delete(e.storage, key)
}
