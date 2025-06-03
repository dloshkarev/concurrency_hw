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
	maxSegmentSize := w.conf.GetMaxSegmentSize()
	var idx int

	writer := bufio.NewWriter(w.segment.file)
	for i, query := range buff {
		querySize := int64(len(query) + 1) // +1 for newline character

		if querySize > maxSegmentSize {
			// Если запрос целиком не влезает в сегмент - падаем
			return fmt.Errorf("query is too large (%d bytes) for max segment size (%d bytes): %s",
				querySize, maxSegmentSize, query)
		}

		if w.segment.size+querySize < maxSegmentSize {
			// Если в сегменте есть место под текущий запрос - пишем на диск
			idx = i + 1
			w.segment.size += querySize

			_, err := writer.WriteString(query + "\n")
			if err != nil {
				return err
			}
		} else {
			// Если текущий запрос не помещается в сегмент - прерываемся и дописываем остаток буфера в следующий
			break
		}
	}

	tail := buff[idx:]

	err := writer.Flush()
	if err != nil {
		return err
	}

	err = w.segment.file.Sync()
	if err != nil {
		return err
	}

	return w.writeRemains(tail)
}

func (w *StringSegmentWriter) writeRemains(remains []string) error {
	if len(remains) > 0 {
		err := w.segment.file.Close()
		if err != nil {
			return err
		}

		// Остаток пишем в следующий файл
		err = w.createNewSegment()
		if err != nil {
			return err
		}

		err = w.Write(remains)
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
