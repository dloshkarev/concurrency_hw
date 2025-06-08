//go:build unit

package partition_test

import (
	"concurrency_hw/internal/database/storage/engine"
	"concurrency_hw/internal/database/storage/engine/mem"
	"concurrency_hw/internal/database/storage/engine/partition"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPartitionedEngine(t *testing.T) {
	t.Run("Create with single partition", func(t *testing.T) {
		engines := []engine.Engine{
			mem.NewInMemoryEngine(10),
		}

		partitionedEngine := partition.NewPartitionedEngine(engines)

		require.NotNil(t, partitionedEngine)
	})

	t.Run("Create with multiple partitions", func(t *testing.T) {
		engines := []engine.Engine{
			mem.NewInMemoryEngine(10),
			mem.NewInMemoryEngine(10),
			mem.NewInMemoryEngine(10),
		}

		partitionedEngine := partition.NewPartitionedEngine(engines)

		require.NotNil(t, partitionedEngine)
	})
}

func TestPartitionedEngine_SetAndGet(t *testing.T) {
	engines := []engine.Engine{
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
	}

	partitionedEngine := partition.NewPartitionedEngine(engines)

	t.Run("Set and Get single key", func(t *testing.T) {
		key := "testkey"
		value := "testvalue"

		partitionedEngine.Set(key, value)
		result := partitionedEngine.Get(key)

		assert.Equal(t, value, result)
	})

	t.Run("Set and Get multiple keys", func(t *testing.T) {
		testCases := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
			"key5": "value5",
		}

		// Set all keys
		for key, value := range testCases {
			partitionedEngine.Set(key, value)
		}

		// Get all keys and verify
		for key, expectedValue := range testCases {
			result := partitionedEngine.Get(key)
			assert.Equal(t, expectedValue, result, "Failed for key: %s", key)
		}
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		result := partitionedEngine.Get("nonexistent")
		assert.Equal(t, "", result)
	})

	t.Run("Overwrite existing key", func(t *testing.T) {
		key := "overwritekey"
		value1 := "value1"
		value2 := "value2"

		partitionedEngine.Set(key, value1)
		assert.Equal(t, value1, partitionedEngine.Get(key))

		partitionedEngine.Set(key, value2)
		assert.Equal(t, value2, partitionedEngine.Get(key))
	})
}

func TestPartitionedEngine_Del(t *testing.T) {
	engines := []engine.Engine{
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
	}

	partitionedEngine := partition.NewPartitionedEngine(engines)

	t.Run("Delete existing key", func(t *testing.T) {
		key := "deletekey"
		value := "deletevalue"

		partitionedEngine.Set(key, value)
		assert.Equal(t, value, partitionedEngine.Get(key))

		partitionedEngine.Del(key)
		assert.Equal(t, "", partitionedEngine.Get(key))
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		key := "nonexistentkey"

		// Should not panic
		partitionedEngine.Del(key)
		assert.Equal(t, "", partitionedEngine.Get(key))
	})

	t.Run("Delete multiple keys", func(t *testing.T) {
		keys := []string{"del1", "del2", "del3", "del4"}

		// Set keys
		for _, key := range keys {
			partitionedEngine.Set(key, "value")
		}

		// Verify keys are set
		for _, key := range keys {
			assert.Equal(t, "value", partitionedEngine.Get(key))
		}

		// Delete keys
		for _, key := range keys {
			partitionedEngine.Del(key)
		}

		// Verify keys are deleted
		for _, key := range keys {
			assert.Equal(t, "", partitionedEngine.Get(key))
		}
	})
}

func TestPartitionedEngine_PartitionDistribution(t *testing.T) {
	engines := []engine.Engine{
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
		mem.NewInMemoryEngine(10),
	}

	partitionedEngine := partition.NewPartitionedEngine(engines)

	t.Run("Keys are distributed across partitions", func(t *testing.T) {
		// Generate many keys to ensure distribution
		keyCount := 30
		keys := make([]string, keyCount)

		for i := 0; i < keyCount; i++ {
			keys[i] = fmt.Sprintf("key%d", i)
			partitionedEngine.Set(keys[i], fmt.Sprintf("value%d", i))
		}

		// Verify all keys can be retrieved
		for i := 0; i < keyCount; i++ {
			expected := fmt.Sprintf("value%d", i)
			actual := partitionedEngine.Get(keys[i])
			assert.Equal(t, expected, actual, "Failed for key: %s", keys[i])
		}
	})

	t.Run("Same key always goes to same partition", func(t *testing.T) {
		key := "consistentkey"
		value1 := "value1"
		value2 := "value2"

		// Set key
		partitionedEngine.Set(key, value1)
		assert.Equal(t, value1, partitionedEngine.Get(key))

		// Update key
		partitionedEngine.Set(key, value2)
		assert.Equal(t, value2, partitionedEngine.Get(key))

		// Delete key
		partitionedEngine.Del(key)
		assert.Equal(t, "", partitionedEngine.Get(key))
	})
}

func TestPartitionedEngine_ConcurrentAccess(t *testing.T) {
	engines := []engine.Engine{
		mem.NewInMemoryEngine(100),
		mem.NewInMemoryEngine(100),
		mem.NewInMemoryEngine(100),
	}

	partitionedEngine := partition.NewPartitionedEngine(engines)

	t.Run("Concurrent operations", func(t *testing.T) {
		const goroutines = 50
		const iterations = 100

		done := make(chan bool, goroutines)
		errors := make(chan error, goroutines)

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				for j := 0; j < iterations; j++ {
					key := fmt.Sprintf("key_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)

					// Set
					partitionedEngine.Set(key, value)

					// Get and verify
					result := partitionedEngine.Get(key)
					if result != value {
						errors <- fmt.Errorf("goroutine %d: expected %s, got %s", id, value, result)
						return
					}

					// Delete
					partitionedEngine.Del(key)

					// Verify deletion
					result = partitionedEngine.Get(key)
					if result != "" {
						errors <- fmt.Errorf("goroutine %d: expected empty after delete, got %s", id, result)
						return
					}
				}
			}(i)
		}

		// Wait for all goroutines to complete
		completed := 0
		for completed < goroutines {
			select {
			case err := <-errors:
				t.Fatalf("Concurrent test failed: %v", err)
			case <-done:
				completed++
			}
		}
	})
}

func TestPartitionedEngine_EdgeCases(t *testing.T) {
	engines := []engine.Engine{
		mem.NewInMemoryEngine(10),
	}

	partitionedEngine := partition.NewPartitionedEngine(engines)

	t.Run("Empty key", func(t *testing.T) {
		key := ""
		value := "emptykey"

		partitionedEngine.Set(key, value)
		result := partitionedEngine.Get(key)
		assert.Equal(t, value, result)

		partitionedEngine.Del(key)
		result = partitionedEngine.Get(key)
		assert.Equal(t, "", result)
	})

	t.Run("Empty value", func(t *testing.T) {
		key := "emptyvalue"
		value := ""

		partitionedEngine.Set(key, value)
		result := partitionedEngine.Get(key)
		assert.Equal(t, value, result)
	})

	t.Run("Very long key and value", func(t *testing.T) {
		longKey := fmt.Sprintf("verylongkey%s", make([]byte, 1000))
		longValue := fmt.Sprintf("verylongvalue%s", make([]byte, 1000))

		partitionedEngine.Set(longKey, longValue)
		result := partitionedEngine.Get(longKey)
		assert.Equal(t, longValue, result)
	})

	t.Run("Special characters in keys", func(t *testing.T) {
		specialKeys := []string{
			"key with spaces",
			"key-with-dashes",
			"key_with_underscores",
			"key.with.dots",
			"key@with@symbols",
			"key/with/slashes",
			"key\\with\\backslashes",
			"key:with:colons",
		}

		for _, key := range specialKeys {
			value := fmt.Sprintf("value-for-%s", key)
			partitionedEngine.Set(key, value)
			result := partitionedEngine.Get(key)
			assert.Equal(t, value, result, "Failed for key: %s", key)
		}
	})
}
