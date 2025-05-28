package mem

import "sync"

type InMemoryEngine struct {
	storage map[string]string
	mu      sync.RWMutex
}

func NewInMemoryEngine(initialSize int) *InMemoryEngine {
	return &InMemoryEngine{
		storage: make(map[string]string, initialSize),
	}
}

func (e *InMemoryEngine) Set(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.storage[key] = value
}

func (e *InMemoryEngine) Get(key string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.storage[key]
}

func (e *InMemoryEngine) Del(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.storage, key)
}
