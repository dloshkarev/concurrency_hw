package wal

import (
	"bufio"
	"concurrency_hw/internal/config"
	"fmt"
	"os"
	"path/filepath"
)

type StringSegmentWriter struct {
	conf    *config.WalConfig
	segment *Segment
}

type SegmentWriter interface {
	Write(buff []string) error
	Close() error
}

func NewStringSegmentWriter(conf *config.WalConfig, segment *Segment) (*StringSegmentWriter, error) {
	return &StringSegmentWriter{
		conf:    conf,
		segment: segment,
	}, nil
}

func (w *StringSegmentWriter) Write(buff []string) error {
	freeSpace := w.conf.GetMaxSegmentSize() - w.segment.size
	var idx int

	for _, query := range buff {
		if freeSpace-int64(len(query)) > 0 {
			freeSpace += int64(len(query))
		} else {
			idx -= 1
			break
		}
		idx++
	}

	tail := buff[idx:]

	// Пишем сколько влезет
	writer := bufio.NewWriter(w.segment.file)
	for _, query := range buff[:idx] {
		_, err := writer.WriteString(query + "\n")
		if err != nil {
			return err
		}
	}

	err := writer.Flush()
	if err != nil {
		return err
	}

	err = w.segment.file.Sync()
	if err != nil {
		return err
	}

	if len(tail) > 0 {
		err := w.segment.file.Close()
		if err != nil {
			return err
		}

		// Остаток пишем в следующий файл
		err = w.createNewSegment()
		if err != nil {
			return err
		}

		err = w.Write(tail)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *StringSegmentWriter) Close() error {
	return w.segment.file.Close()
}

func (w *StringSegmentWriter) createNewSegment() error {
	newSegmentFileName := fmt.Sprintf("%d.%s", w.segment.segmentNum+1, extension)
	segmentFile, err := os.Create(filepath.Join(w.conf.DataDirectory, newSegmentFileName))
	if err != nil {
		return err
	}

	w.segment.segmentNum += 1
	w.segment.size = 0
	w.segment.file = segmentFile

	return nil
}

func createDirIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

func createSegmentFileIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		segmentFile, err := os.Create(path)
		if err != nil {
			return err
		}

		err = segmentFile.Close()
		if err != nil {
			return err
		}

		return nil
	}

	return nil
}
