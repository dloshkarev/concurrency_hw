package mem

import (
	"fmt"
	"testing"
)

func TestInMemoryEngine(t *testing.T) {
	initialSize := 10
	engine := NewInMemoryEngine(initialSize)

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
	engine := NewInMemoryEngine(0)

	t.Run("Concurrent access", func(t *testing.T) {
		const goroutines = 100
		const iterations = 100
		done := make(chan bool)
		errors := make(chan error, goroutines)

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				for j := 0; j < iterations; j++ {
					key := fmt.Sprintf("key_%d", id)
					value := fmt.Sprintf("value_%d_%d", id, j)

					// Set value
					engine.Set(key, value)

					// Get and verify value
					got := engine.Get(key)
					if got != value {
						errors <- fmt.Errorf("goroutine %d: expected value %s, got %s", id, value, got)
						return
					}

					// Delete value
					engine.Del(key)

					// Verify deletion
					got = engine.Get(key)
					if got != "" {
						errors <- fmt.Errorf("goroutine %d: expected empty value after deletion, got %s", id, got)
						return
					}
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < goroutines; i++ {
			select {
			case err := <-errors:
				t.Fatalf("Concurrency test failed: %v", err)
			case <-done:
				// Goroutine completed successfully
			}
		}
	})
}
