package mem

import (
	"testing"

	"go.uber.org/zap"
)

func TestInMemoryEngine(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	initialSize := 10
	engine := NewEngine(logger, initialSize)

	t.Run("Set and Get", func(t *testing.T) {
		key := "testKey"
		value := "testValue"

		engine.Set(key, value)
		got := engine.Get(key)

		if got != value {
			t.Errorf("Get() = %v, want %v", got, value)
		}
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		key := "nonExistentKey"
		got := engine.Get(key)

		if got != "" {
			t.Errorf("Get() for non-existent key = %v, want empty string", got)
		}
	})

	t.Run("Delete key", func(t *testing.T) {
		key := "keyToDelete"
		value := "valueToDelete"

		engine.Set(key, value)
		engine.Del(key)
		got := engine.Get(key)

		if got != "" {
			t.Errorf("Get() after Del() = %v, want empty string", got)
		}
	})

	t.Run("Overwrite value", func(t *testing.T) {
		key := "sameKey"
		value1 := "firstValue"
		value2 := "secondValue"

		engine.Set(key, value1)
		engine.Set(key, value2)
		got := engine.Get(key)

		if got != value2 {
			t.Errorf("Get() after overwrite = %v, want %v", got, value2)
		}
	})
}

func TestConcurrency(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	engine := NewEngine(logger, 0)

	// TODO: Тест на конкурентный доступ пока не работает
	t.Run("Concurrent access", func(t *testing.T) {
		const goroutines = 100
		done := make(chan bool)

		for i := 0; i < goroutines; i++ {
			go func(i int) {
				key := "key"
				value := "value"
				engine.Set(key, value)
				engine.Get(key)
				engine.Del(key)
				done <- true
			}(i)
		}

		for i := 0; i < goroutines; i++ {
			<-done
		}
	})
}
