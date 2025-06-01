package wal

import (
	"concurrency_hw/internal/config"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestNewSegmentedFSWal тестирует корректность создания нового SegmentedFSWal
// с проверкой правильной инициализации всех полей структуры
func TestNewSegmentedFSWal(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    100,
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	// Создаем сегмент через reader
	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	if wal.conf != conf {
		t.Errorf("Expected conf to be set")
	}

	if wal.logger != logger {
		t.Errorf("Expected logger to be set")
	}

	if wal.reader != reader {
		t.Errorf("Expected reader to be set")
	}

	if wal.writer != writer {
		t.Errorf("Expected writer to be set")
	}

	expectedBuffLen := int(1.1 * float64(conf.FlushingBatchSize))
	if cap(wal.buff) != expectedBuffLen {
		t.Errorf("Expected buffer capacity to be %d, got %d", expectedBuffLen, cap(wal.buff))
	}
}

// TestSegmentedFSWal_Append тестирует добавление записей в WAL
// Проверяет корректность буферизации и автоматического flush при достижении лимита
func TestSegmentedFSWal_Append(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    3, // Маленький размер для тестирования
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	// Добавляем записи
	queries := []string{
		"SET key1 value1",
		"SET key2 value2",
		"SET key3 value3", // Это должно вызвать flush
	}

	for _, query := range queries {
		err = wal.Append(query)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// После автоматического flush буфер должен быть пустым
	if len(wal.buff) != 0 {
		t.Errorf("Expected buffer to be empty after auto flush, got %d items", len(wal.buff))
	}
}

// TestSegmentedFSWal_Append_TooLarge тестирует обработку слишком больших записей
// Проверяет корректность возврата ошибки при превышении максимального размера сегмента
func TestSegmentedFSWal_Append_TooLarge(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "20b", // Очень маленький размер
		DataDirectory:        tempDir,
		FlushingBatchSize:    100,
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	// Пытаемся добавить слишком большую запись
	largeQuery := "SET key " + strings.Repeat("x", 100) // Больше 20 байт

	err = wal.Append(largeQuery)
	if err == nil {
		t.Fatalf("Expected Append() to return error for large query")
	}

	if !strings.Contains(err.Error(), "larger than max segment size") {
		t.Errorf("Expected error message to contain 'larger than max segment size', got: %s", err.Error())
	}
}

// TestSegmentedFSWal_ForEach тестирует итерацию по записям WAL
// Проверяет корректность делегирования к reader и чтения ранее записанных данных
func TestSegmentedFSWal_ForEach(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Сначала создаем файлы с данными
	testData := []string{
		"SET key1 value1",
		"SET key2 value2",
		"GET key1",
	}

	segmentPath := filepath.Join(tempDir, "0.seg")
	file, err := os.Create(segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment file: %v", err)
	}

	for _, query := range testData {
		_, err = file.WriteString(query + "\n")
		if err != nil {
			if closeErr := file.Close(); closeErr != nil {
				t.Errorf("Failed to close file: %v", closeErr)
			}
			t.Fatalf("Failed to write to file: %v", err)
		}
	}

	if closeErr := file.Close(); closeErr != nil {
		t.Errorf("Failed to close file: %v", closeErr)
	}

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    100,
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	var collectedQueries []string
	err = wal.ForEach(func(queryString string) error {
		collectedQueries = append(collectedQueries, queryString)
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach() error = %v", err)
	}

	if len(collectedQueries) != len(testData) {
		t.Fatalf("Expected %d queries, got %d", len(testData), len(collectedQueries))
	}

	for i, expected := range testData {
		if collectedQueries[i] != expected {
			t.Errorf("Expected query at index %d to be '%s', got '%s'", i, expected, collectedQueries[i])
		}
	}
}

// TestSegmentedFSWal_ForEach_CallbackError тестирует обработку ошибки в callback функции ForEach
// Проверяет корректность возврата ошибки из callback функции
func TestSegmentedFSWal_ForEach_CallbackError(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем файл с данными
	segmentPath := filepath.Join(tempDir, "0.seg")
	file, err := os.Create(segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment file: %v", err)
	}

	_, err = file.WriteString("SET key1 value1\n")
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("Failed to close file: %v", closeErr)
		}
		t.Fatalf("Failed to write to file: %v", err)
	}

	if closeErr := file.Close(); closeErr != nil {
		t.Errorf("Failed to close file: %v", closeErr)
	}

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    100,
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	expectedError := "callback error"
	err = wal.ForEach(func(queryString string) error {
		return errors.New(expectedError)
	})

	if err == nil {
		t.Fatalf("Expected ForEach() to return error from callback")
	}

	if err.Error() != expectedError {
		t.Errorf("Expected error message to be '%s', got '%s'", expectedError, err.Error())
	}
}

// TestSegmentedFSWal_Close тестирует корректное закрытие WAL
// Проверяет, что все данные сбрасываются на диск и ресурсы освобождаются
func TestSegmentedFSWal_Close(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    100, // Большой размер, чтобы данные остались в буфере
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	// Добавляем данные в буфер, но не до лимита flush
	queries := []string{
		"SET key1 value1",
		"SET key2 value2",
	}

	for _, query := range queries {
		err = wal.Append(query)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Убеждаемся, что данные в буфере
	if len(wal.buff) != len(queries) {
		t.Errorf("Expected %d items in buffer, got %d", len(queries), len(wal.buff))
	}

	// Закрываем WAL - должен произойти flush
	err = wal.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Проверяем, что данные записались на диск
	segmentPath := filepath.Join(tempDir, "0")
	content, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("Failed to read segment file: %v", err)
	}

	contentStr := string(content)
	for _, query := range queries {
		if !strings.Contains(contentStr, query) {
			t.Errorf("Expected segment file to contain '%s', got: %s", query, contentStr)
		}
	}
}

// TestSegmentedFSWal_IntegrationAppendAndRead тестирует полный цикл записи и чтения
// Проверяет интеграцию между записью через Append и чтением через ForEach
func TestSegmentedFSWal_IntegrationAppendAndRead(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    2, // Маленький размер для принудительного flush
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	// Записываем данные
	testQueries := []string{
		"SET key1 value1",
		"SET key2 value2", // Здесь должен произойти flush
		"GET key1",
		"DEL key2", // И здесь тоже
	}

	for _, query := range testQueries {
		err = wal.Append(query)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Принудительно закрываем, чтобы сбросить оставшиеся данные
	err = wal.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Создаем новый WAL для чтения
	reader2 := NewStringSegmentReader(conf)
	segment2, err := reader2.Open()
	if err != nil {
		t.Fatalf("Failed to open segment for reading: %v", err)
	}

	writer2, err := NewStringSegmentWriter(conf, segment2)
	if err != nil {
		t.Fatalf("Failed to create writer for reading: %v", err)
	}

	wal2, err := NewSegmentedFSWal(conf, logger, reader2, writer2)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() for reading error = %v", err)
	}

	defer func() {
		if closeErr := wal2.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL2: %v", closeErr)
		}
	}()

	// Читаем данные
	var readQueries []string
	err = wal2.ForEach(func(queryString string) error {
		readQueries = append(readQueries, queryString)
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach() error = %v", err)
	}

	// Проверяем, что прочитанные данные совпадают с записанными
	if len(readQueries) != len(testQueries) {
		t.Fatalf("Expected %d queries, got %d", len(testQueries), len(readQueries))
	}

	for i, expected := range testQueries {
		if readQueries[i] != expected {
			t.Errorf("Expected query at index %d to be '%s', got '%s'", i, expected, readQueries[i])
		}
	}
}

// TestSegmentedFSWal_Append_BufferNotFull тестирует добавление записей без достижения лимита flush
// Проверяет, что данные остаются в буфере до принудительного flush
func TestSegmentedFSWal_Append_BufferNotFull(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    10, // Большой размер, чтобы не было автоматического flush
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	// Добавляем записи, но не до лимита flush
	queries := []string{
		"SET key1 value1",
		"SET key2 value2",
	}

	for _, query := range queries {
		err = wal.Append(query)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Данные должны остаться в буфере
	if len(wal.buff) != len(queries) {
		t.Errorf("Expected %d items in buffer, got %d", len(queries), len(wal.buff))
	}
}

// TestSegmentedFSWal_FlushError тестирует обработку ошибки при flush
// Проверяет корректность обработки ошибок записи
func TestSegmentedFSWal_FlushError(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    1, // Маленький размер для немедленного flush
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	// Закрываем writer, чтобы вызвать ошибку при записи
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Пытаемся добавить запись - должна произойти ошибка при flush
	err = wal.Append("SET key1 value1")
	if err == nil {
		t.Fatalf("Expected Append() to return error when writer is closed")
	}
}

// TestSegmentedFSWal_MultipleFlushes тестирует множественные flush операции
// Проверяет корректность работы при многократном достижении лимита буфера
func TestSegmentedFSWal_MultipleFlushes(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize:       "1KB",
		DataDirectory:        tempDir,
		FlushingBatchSize:    2, // Маленький размер для частых flush
		FlushingBatchTimeout: 10 * time.Millisecond,
	}

	logger, _ := zap.NewDevelopment()
	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	wal, err := NewSegmentedFSWal(conf, logger, reader, writer)
	if err != nil {
		t.Fatalf("NewSegmentedFSWal() error = %v", err)
	}

	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Failed to close WAL: %v", closeErr)
		}
	}()

	// Добавляем записи, которые вызовут несколько flush операций
	queries := []string{
		"SET key1 value1", // 1
		"SET key2 value2", // 2 - flush
		"SET key3 value3", // 1
		"SET key4 value4", // 2 - flush
		"SET key5 value5", // 1
	}

	for _, query := range queries {
		err = wal.Append(query)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// В буфере должна остаться только последняя запись
	if len(wal.buff) != 1 {
		t.Errorf("Expected 1 item in buffer, got %d", len(wal.buff))
	}

	if wal.buff[0] != "SET key5 value5" {
		t.Errorf("Expected last item to be 'SET key5 value5', got '%s'", wal.buff[0])
	}
}
