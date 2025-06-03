//go:build unit

package wal

import (
	"concurrency_hw/internal/config"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStringSegmentWriter_Write_SingleSegment тестирует запись данных, которые помещаются в один сегмент
// Проверяет корректность записи содержимого в файл и правильность формата данных
func TestStringSegmentWriter_Write_SingleSegment(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	// Создаем конфиг с большим размером сегмента
	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	// Создаем файл сегмента
	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 1024,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Тестируем запись небольшого количества данных
	testData := []string{
		"SET key1 value1",
		"SET key2 value2",
		"GET key1",
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем, что данные записались в файл
	_, err = segmentFile.Seek(0, 0)
	if err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}

	content, err := io.ReadAll(segmentFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expectedContent := "SET key1 value1\nSET key2 value2\nGET key1\n"
	if string(content) != expectedContent {
		t.Errorf("Expected content:\n%s\nGot:\n%s", expectedContent, string(content))
	}
}

// TestStringSegmentWriter_Write_MultipleSegments тестирует запись данных, требующих создания нескольких сегментов
// Проверяет корректность создания новых файлов сегментов при превышении лимита размера
func TestStringSegmentWriter_Write_MultipleSegments(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	// Создаем конфиг с очень маленьким размером сегмента
	conf := &config.WalConfig{
		MaxSegmentSize: "50b", // 50 байт
		DataDirectory:  tempDir,
	}

	// Создаем первый файл сегмента
	segmentPath := filepath.Join(tempDir, "1.seg")
	segmentFile, err := os.Create(segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment file: %v", err)
	}

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 50,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Тестируем запись данных, которые не помещаются в один сегмент
	// Каждая строка помещается в сегмент, но все вместе требуют несколько сегментов
	testData := []string{
		"SET key1 value1", // 16 байт + 1 = 17 байт
		"SET key2 value2", // 16 байт + 1 = 17 байт (всего 34, поместится)
		"SET key3 value3", // 16 байт + 1 = 17 байт (всего 51, не поместится - пойдет в новый сегмент)
		"GET key1",        // 9 байт + 1 = 10 байт (поместится в сегмент 2)
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем, что создался второй сегмент
	secondSegmentPath := filepath.Join(tempDir, "2.seg")
	if _, err := os.Stat(secondSegmentPath); os.IsNotExist(err) {
		t.Errorf("Expected second segment file to be created")
	}
}

// TestStringSegmentWriter_Write_EmptyBuffer тестирует обработку пустого буфера для записи
// Проверяет, что метод Write корректно обрабатывает пустой slice без ошибок
func TestStringSegmentWriter_Write_EmptyBuffer(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 1024,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Тестируем запись пустого буфера
	err = writer.Write([]string{})
	if err != nil {
		t.Fatalf("Write() with empty buffer should not error, got: %v", err)
	}
}

// TestStringSegmentWriter_Close тестирует корректность закрытия StringSegmentWriter
// Проверяет, что все ресурсы освобождаются без ошибок
func TestStringSegmentWriter_Close(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	segmentFile, err := os.CreateTemp(tempDir, "segment*.seg")
	if err != nil {
		t.Fatalf("Failed to create segment file: %v", err)
	}

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 1024,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Тестируем закрытие
	err = writer.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestStringSegmentWriter_createNewSegment тестирует создание нового сегмента
// Проверяет обновление номера сегмента, сброс размера и создание нового файла
func TestStringSegmentWriter_createNewSegment(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "1KB",
		DataDirectory:  tempDir,
	}

	// Создаем первый файл сегмента
	initialFile, err := os.CreateTemp(tempDir, "segment*.seg")
	if err != nil {
		t.Fatalf("Failed to create initial segment file: %v", err)
	}

	defer func() {
		_ = initialFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           initialFile,
		size:           100,
		maxSegmentSize: 1024,
	}

	writer := &StringSegmentWriter{
		conf:    conf,
		segment: segment,
	}

	// Тестируем создание нового сегмента
	err = writer.createNewSegment()
	if err != nil {
		t.Fatalf("createNewSegment() error = %v", err)
	}

	// Проверяем, что номер сегмента увеличился
	if writer.segment.segmentNum != 2 {
		t.Errorf("Expected segment number to be 2, got %d", writer.segment.segmentNum)
	}

	// Проверяем, что размер сегмента сброшен
	if writer.segment.size != 0 {
		t.Errorf("Expected segment size to be 0, got %d", writer.segment.size)
	}

	// Проверяем, что новый файл создался
	expectedPath := filepath.Join(tempDir, "2.seg")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected new segment file to be created at %s", expectedPath)
	}

}

// TestStringSegmentWriter_Write_CalculateFreeSpace тестирует логику расчета свободного места в сегменте
// Проверяет, что данные корректно разделяются между текущим и новым сегментами
func TestStringSegmentWriter_Write_CalculateFreeSpace(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "50b", // Очень маленький размер для тестирования
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	// Сегмент уже наполовину заполнен
	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           25, // Половина от 50 байт
		maxSegmentSize: 50,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Пытаемся записать данные, которые частично поместятся
	testData := []string{
		"short",             // 5 символов + \n = 6 байт (поместится)
		"a bit longer text", // 18 символов + \n = 19 байт (не поместится)
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем содержимое первого файла
	_, err = segmentFile.Seek(0, 0)
	if err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}

	content, err := io.ReadAll(segmentFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// В первом файле должна быть только первая строка
	if !strings.Contains(string(content), "short\n") {
		t.Errorf("Expected first segment to contain 'short', got: %s", string(content))
	}
}

// TestCreateDirIfNotExists тестирует создание директории, если она не существует
// Проверяет корректность работы с существующими и несуществующими директориями
func TestCreateDirIfNotExists(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	// Тестируем создание новой директории
	newDir := filepath.Join(tempDir, "new_dir")
	err := createDirIfNotExists(newDir)
	if err != nil {
		t.Fatalf("createDirIfNotExists() error = %v", err)
	}

	// Проверяем, что директория создалась
	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		t.Errorf("Expected directory to be created")
	}

	// Тестируем с существующей директорией
	err = createDirIfNotExists(newDir)
	if err != nil {
		t.Fatalf("createDirIfNotExists() with existing dir error = %v", err)
	}
}

// TestCreateSegmentFileIfNotExists тестирует создание файла сегмента, если он не существует
// Проверяет корректность работы с существующими и несуществующими файлами
func TestCreateSegmentFileIfNotExists(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	// Тестируем создание нового файла
	newFile := filepath.Join(tempDir, "test.seg")
	err := createSegmentFileIfNotExists(newFile)
	if err != nil {
		t.Fatalf("createSegmentFileIfNotExists() error = %v", err)
	}

	// Проверяем, что файл создался
	if _, err := os.Stat(newFile); os.IsNotExist(err) {
		t.Errorf("Expected file to be created")
	}

	// Тестируем с существующим файлом
	err = createSegmentFileIfNotExists(newFile)
	if err != nil {
		t.Fatalf("createSegmentFileIfNotExists() with existing file error = %v", err)
	}
}

// TestStringSegmentWriter_Write_ExactFit тестирует запись данных, которые точно заполняют сегмент
// Проверяет граничный случай, когда данные полностью используют доступное место в сегменте
func TestStringSegmentWriter_Write_ExactFit(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	// Создаем конфиг с размером ровно под наши данные
	conf := &config.WalConfig{
		MaxSegmentSize: "20b", // 20 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 20,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Записываем данные, которые точно помещаются в сегмент
	// "test\n" = 5 байт, "data\n" = 5 байт, "end\n" = 4 байт = 14 байт всего
	testData := []string{
		"test",
		"data",
		"end",
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем содержимое
	_, err = segmentFile.Seek(0, 0)
	if err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}

	content, err := io.ReadAll(segmentFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expectedContent := "test\ndata\nend\n"
	if string(content) != expectedContent {
		t.Errorf("Expected content:\n%s\nGot:\n%s", expectedContent, string(content))
	}
}

// TestStringSegmentWriter_Write_SingleLargeQuery тестирует запись одного запроса, не помещающегося в текущий сегмент
// Проверяет корректность создания нового сегмента для большого запроса
func TestStringSegmentWriter_Write_SingleLargeQuery(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "20b", // 20 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	// Сегмент уже частично заполнен
	_, err := segmentFile.WriteString("existing\n") // 9 байт
	if err != nil {
		t.Fatalf("Failed to write existing data: %v", err)
	}

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           9, // 9 байт уже использовано
		maxSegmentSize: 20,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Записываем запрос, который не помещается в текущий сегмент (9 + 15 = 24 > 20)
	// но поместится в новый сегмент
	testData := []string{
		"medium_query", // 12 символов + 1 = 13 байт
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем, что создался второй сегмент для этого запроса
	secondSegmentPath := filepath.Join(tempDir, "2.seg")
	if _, err := os.Stat(secondSegmentPath); os.IsNotExist(err) {
		t.Errorf("Expected second segment file to be created")
	}
}

// TestStringSegmentWriter_Write_PrefilledSegment тестирует запись в уже частично заполненный сегмент
// Проверяет корректность добавления данных к существующему содержимому сегмента
func TestStringSegmentWriter_Write_PrefilledSegment(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "30b", // 30 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	// Записываем что-то в файл заранее
	_, err := segmentFile.WriteString("existing\n")
	if err != nil {
		t.Fatalf("Failed to write existing data: %v", err)
	}

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           9, // "existing\n" = 9 байт
		maxSegmentSize: 30,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Добавляем данные в уже заполненный сегмент
	testData := []string{
		"new1", // 5 байт
		"new2", // 5 байт
		"new3", // 5 байт
	}
	// Всего: 9 (существующие) + 5 + 5 + 5 = 24 байта (должно поместиться)

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем содержимое
	_, err = segmentFile.Seek(0, 0)
	if err != nil {
		t.Fatalf("Failed to seek file: %v", err)
	}

	content, err := io.ReadAll(segmentFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expectedContent := "existing\nnew1\nnew2\nnew3\n"
	if string(content) != expectedContent {
		t.Errorf("Expected content:\n%s\nGot:\n%s", expectedContent, string(content))
	}
}

// TestStringSegmentWriter_Write_RecursiveSegmentCreation тестирует создание множественных сегментов при большом объеме данных
// Проверяет корректность рекурсивного создания сегментов при записи большого количества данных
func TestStringSegmentWriter_Write_RecursiveSegmentCreation(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "15b", // 15 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 15,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Записываем данные, которые потребуют создания нескольких сегментов
	testData := []string{
		"query1", // 7 байт (поместится в сегмент 1)
		"query2", // 7 байт (поместится в сегмент 1)
		"query3", // 7 байт (пойдет в сегмент 2)
		"query4", // 7 байт (поместится в сегмент 2)
		"query5", // 7 байт (пойдет в сегмент 3)
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем, что создались дополнительные сегменты
	secondSegmentPath := filepath.Join(tempDir, "2.seg")
	thirdSegmentPath := filepath.Join(tempDir, "3.seg")

	if _, err := os.Stat(secondSegmentPath); os.IsNotExist(err) {
		t.Errorf("Expected second segment file to be created")
	}

	if _, err := os.Stat(thirdSegmentPath); os.IsNotExist(err) {
		t.Errorf("Expected third segment file to be created")
	}
}

// TestStringSegmentWriter_Write_UpdateSegmentSize тестирует обновление размера сегмента после записи
// Проверяет, что поле size корректно отслеживает количество записанных данных
func TestStringSegmentWriter_Write_UpdateSegmentSize(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "100b", // 100 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 100,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	initialSize := segment.size

	// Записываем данные
	testData := []string{
		"test query", // 10 символов + 1 новая строка = 11 байт
	}

	err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Проверяем, что размер сегмента обновился
	expectedSize := initialSize + 11 // "test query\n" = 11 байт
	if segment.size != expectedSize {
		t.Errorf("Expected segment size to be %d, got %d", expectedSize, segment.size)
	}
}

// TestStringSegmentWriter_Write_QueryTooLarge тестирует обработку запроса, превышающего максимальный размер сегмента
// Проверяет корректность возврата ошибки при попытке записи слишком большого запроса
func TestStringSegmentWriter_Write_QueryTooLarge(t *testing.T) {
	// Создаем временную директорию
	tempDir := createTmpDir(t)

	conf := &config.WalConfig{
		MaxSegmentSize: "20b", // 20 байт
		DataDirectory:  tempDir,
	}

	segmentFile := createSegmentFile(t, tempDir)

	defer func() {
		_ = segmentFile.Close()
		_ = os.RemoveAll(tempDir)
	}()

	segment := &Segment{
		segmentNum:     1,
		file:           segmentFile,
		size:           0,
		maxSegmentSize: 20,
	}

	writer, err := NewStringSegmentWriter(conf, segment)
	if err != nil {
		t.Fatalf("NewStringSegmentWriter() error = %v", err)
	}

	// Пытаемся записать запрос, который больше максимального размера сегмента
	testData := []string{
		"this_is_a_very_long_query_that_definitely_exceeds_the_maximum_segment_size_limit",
	}

	err = writer.Write(testData)
	if err == nil {
		t.Fatalf("Write() should have returned an error for query too large")
	}

	// Проверяем, что ошибка содержит ожидаемый текст
	if !strings.Contains(err.Error(), "too large") {
		t.Errorf("Expected error message to contain 'too large', got: %s", err.Error())
	}
}

func createTmpDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	return tempDir
}

func createSegmentFile(t *testing.T, tempDir string) *os.File {
	// Создаем файл сегмента
	segmentFile, err := os.CreateTemp(tempDir, "segment*.seg")
	if err != nil {
		t.Fatalf("Failed to create segment file: %v", err)
	}

	return segmentFile
}
