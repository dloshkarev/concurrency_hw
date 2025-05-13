package engine

type Engine interface {
	Set(key, value string)
	Get(key string) string
	Del(key string)
}
