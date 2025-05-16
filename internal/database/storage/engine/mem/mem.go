package mem

// TODO: о потокобезопасности речи пока не идет
type InMemoryEngine struct {
	storage map[string]string
}

func NewEngine(initialSize int) *InMemoryEngine {
	return &InMemoryEngine{
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
