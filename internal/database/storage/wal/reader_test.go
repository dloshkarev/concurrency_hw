package wal

import (
	"concurrency_hw/internal/config"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestNewStringSegmentReader тестирует корректность создания нового StringSegmentReader
// с проверкой правильной инициализации всех полей структуры
func TestNewStringSegmentReader(t *testing.T) {
	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  "/tmp/test",
	}

	reader := NewStringSegmentReader(conf)

	if reader.conf != conf {
		t.Errorf("Expected conf to be set")
	}
}

// TestStringSegmentReader_Open тестирует открытие последнего сегмента
// Проверяет корректность создания директории, файла и возврата структуры Segment
func TestStringSegmentReader_Open(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	defer func() {
		if closeErr := segment.file.Close(); closeErr != nil {
			t.Errorf("Failed to close segment file: %v", closeErr)
		}
	}()

	if segment == nil {
		t.Fatalf("Expected segment to be non-nil")
	}

	if segment.segmentNum != 0 {
		t.Errorf("Expected segment number to be 0, got %d", segment.segmentNum)
	}

	if segment.maxSegmentSize != 1024 {
		t.Errorf("Expected max segment size to be 1024, got %d", segment.maxSegmentSize)
	}
}

// TestStringSegmentReader_Open_ExistingSegments тестирует открытие при наличии существующих сегментов
// Проверяет, что открывается последний сегмент по номеру
func TestStringSegmentReader_Open_ExistingSegments(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем несколько файлов сегментов
	segmentFiles := []string{"0.seg", "1.seg", "2.seg"}
	for _, filename := range segmentFiles {
		path := filepath.Join(tempDir, filename)
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("Failed to create segment file %s: %v", filename, err)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("Failed to close file %s: %v", filename, closeErr)
		}
	}

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	reader := NewStringSegmentReader(conf)

	segment, err := reader.Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	defer func() {
		if closeErr := segment.file.Close(); closeErr != nil {
			t.Errorf("Failed to close segment file: %v", closeErr)
		}
	}()

	// Должен открыться последний сегмент (с номером 2)
	if segment.segmentNum != 2 {
		t.Errorf("Expected segment number to be 2, got %d", segment.segmentNum)
	}
}

// TestStringSegmentReader_ForEach тестирует итерацию по всем записям во всех сегментах
// Проверяет корректность чтения данных из нескольких файлов сегментов
func TestStringSegmentReader_ForEach(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем несколько файлов сегментов с данными
	testData := map[string][]string{
		"0.seg": {"SET key1 value1", "SET key2 value2"},
		"1.seg": {"GET key1", "DEL key2"},
		"2.seg": {"SET key3 value3"},
	}

	for filename, queries := range testData {
		path := filepath.Join(tempDir, filename)
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("Failed to create segment file %s: %v", filename, err)
		}

		for _, query := range queries {
			_, err = file.WriteString(query + "\n")
			if err != nil {
				if closeErr := file.Close(); closeErr != nil {
					t.Errorf("Failed to close file %s: %v", filename, closeErr)
				}
				t.Fatalf("Failed to write to file %s: %v", filename, err)
			}
		}

		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("Failed to close file %s: %v", filename, closeErr)
		}
	}

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	reader := NewStringSegmentReader(conf)

	var collectedQueries []string
	err := reader.ForEach(func(queryString string) error {
		collectedQueries = append(collectedQueries, queryString)
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach() error = %v", err)
	}

	// Проверяем, что все запросы были прочитаны в правильном порядке
	expectedQueries := []string{
		"SET key1 value1", "SET key2 value2", // из 0.seg
		"GET key1", "DEL key2", // из 1.seg
		"SET key3 value3", // из 2.seg
	}

	if len(collectedQueries) != len(expectedQueries) {
		t.Fatalf("Expected %d queries, got %d", len(expectedQueries), len(collectedQueries))
	}

	for i, expected := range expectedQueries {
		if collectedQueries[i] != expected {
			t.Errorf("Expected query at index %d to be '%s', got '%s'", i, expected, collectedQueries[i])
		}
	}
}

// TestStringSegmentReader_ForEach_EmptyDirectory тестирует итерацию по пустой директории
// Проверяет корректность обработки случая, когда нет файлов сегментов
func TestStringSegmentReader_ForEach_EmptyDirectory(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	reader := NewStringSegmentReader(conf)

	var callCount int
	err := reader.ForEach(func(queryString string) error {
		callCount++
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach() error = %v", err)
	}

	if callCount != 0 {
		t.Errorf("Expected no calls to callback function, got %d", callCount)
	}
}

// TestStringSegmentReader_ForEach_CallbackError тестирует обработку ошибки в callback функции
// Проверяет, что ошибка из callback функции корректно возвращается
func TestStringSegmentReader_ForEach_CallbackError(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем файл сегмента с данными
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
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	reader := NewStringSegmentReader(conf)

	expectedError := errors.New("callback error")
	err = reader.ForEach(func(queryString string) error {
		return expectedError
	})

	if err == nil {
		t.Fatalf("Expected ForEach() to return error from callback")
	}

	if err != expectedError {
		t.Errorf("Expected error to be '%v', got '%v'", expectedError, err)
	}
}

// TestGetSegmentNum тестирует извлечение номера сегмента из пути к файлу
// Проверяет корректность парсинга номера из различных форматов имен файлов
func TestGetSegmentNum(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     int
		wantErr  bool
	}{
		{
			name:     "Valid segment file with .seg extension",
			filePath: "/path/to/1.seg",
			want:     1,
			wantErr:  false,
		},
		{
			name:     "Valid segment file without extension",
			filePath: "/path/to/42",
			want:     42,
			wantErr:  false,
		},
		{
			name:     "Zero segment number",
			filePath: "/path/to/0.seg",
			want:     0,
			wantErr:  false,
		},
		{
			name:     "Invalid segment number",
			filePath: "/path/to/invalid.seg",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "Empty filename",
			filePath: "/path/to/.seg",
			want:     0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getSegmentNum(tt.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSegmentNum() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getSegmentNum() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFindSortedSegments тестирует поиск и сортировку файлов сегментов в директории
// Проверяет корректность сортировки файлов по номерам сегментов
func TestFindSortedSegments(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем файлы сегментов в случайном порядке
	segmentFiles := []string{"10.seg", "1.seg", "2.seg", "0.seg"}
	for _, filename := range segmentFiles {
		path := filepath.Join(tempDir, filename)
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("Failed to create segment file %s: %v", filename, err)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("Failed to close file %s: %v", filename, closeErr)
		}
	}

	sortedPaths, err := findSortedSegments(tempDir)
	if err != nil {
		t.Fatalf("findSortedSegments() error = %v", err)
	}

	if len(sortedPaths) != len(segmentFiles) {
		t.Fatalf("Expected %d segment files, got %d", len(segmentFiles), len(sortedPaths))
	}

	// Проверяем, что файлы отсортированы по номерам
	expectedOrder := []string{"0.seg", "1.seg", "2.seg", "10.seg"}
	for i, expectedFilename := range expectedOrder {
		actualFilename := filepath.Base(sortedPaths[i])
		if actualFilename != expectedFilename {
			t.Errorf("Expected file at index %d to be %s, got %s", i, expectedFilename, actualFilename)
		}
	}
}

// TestFindSortedSegments_EmptyDirectory тестирует поиск сегментов в пустой директории
// Проверяет корректность обработки случая, когда в директории нет файлов
func TestFindSortedSegments_EmptyDirectory(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	sortedPaths, err := findSortedSegments(tempDir)
	if err != nil {
		t.Fatalf("findSortedSegments() error = %v", err)
	}

	if len(sortedPaths) != 0 {
		t.Errorf("Expected empty slice for empty directory, got %d files", len(sortedPaths))
	}
}

// TestFindLastSegmentPath тестирует поиск пути к последнему сегменту
// Проверяет корректность определения последнего сегмента среди существующих файлов
func TestFindLastSegmentPath(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	// Создаем несколько файлов сегментов
	segmentFiles := []string{"0.seg", "5.seg", "2.seg"}
	for _, filename := range segmentFiles {
		path := filepath.Join(tempDir, filename)
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("Failed to create segment file %s: %v", filename, err)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("Failed to close file %s: %v", filename, closeErr)
		}
	}

	lastPath, err := findLastSegmentPath(tempDir)
	if err != nil {
		t.Fatalf("findLastSegmentPath() error = %v", err)
	}

	expectedFilename := "5.seg"
	actualFilename := filepath.Base(lastPath)
	if actualFilename != expectedFilename {
		t.Errorf("Expected last segment to be %s, got %s", expectedFilename, actualFilename)
	}
}

// TestFindLastSegmentPath_EmptyDirectory тестирует поиск последнего сегмента в пустой директории
// Проверяет, что возвращается путь к сегменту "0" при отсутствии файлов
func TestFindLastSegmentPath_EmptyDirectory(t *testing.T) {
	tempDir := createTmpDir(t)
	defer cleanupDir(t, tempDir)

	lastPath, err := findLastSegmentPath(tempDir)
	if err != nil {
		t.Fatalf("findLastSegmentPath() error = %v", err)
	}

	expectedFilename := "0"
	actualFilename := filepath.Base(lastPath)
	if actualFilename != expectedFilename {
		t.Errorf("Expected default segment to be %s, got %s", expectedFilename, actualFilename)
	}
}

// cleanupDir безопасно удаляет директорию с проверкой ошибок
func cleanupDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to cleanup directory %s: %v", dir, err)
	}
}
