package wal

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database/compute"
	"errors"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

const extension = "seg"

var WalCommands = map[compute.CommandId]bool{
	compute.SetCommandId: true,
	compute.DelCommandId: true,
}

type Wal interface {
	ForEach(func(string) error) error
	Append(string) error
	Close() error
}

type Segment struct {
	segmentNum     int
	file           *os.File
	size           int64
	maxSegmentSize int64
}

type SegmentedFSWal struct {
	conf     *config.WalConfig
	logger   *zap.Logger
	reader   SegmentReader
	writer   SegmentWriter
	buffSize int
	buff     []string
	segment  *Segment
	ticker   *time.Ticker
	mu       *sync.Mutex
}

func NewSegmentedFSWal(
	conf *config.WalConfig,
	logger *zap.Logger,
	segmentReader SegmentReader,
	segmentWriter SegmentWriter,
) (*SegmentedFSWal, error) {
	segment, err := segmentReader.Open()
	if err != nil {
		return nil, err
	}

	buffLen := int(1.1 * float64(conf.FlushingBatchSize))

	wal := &SegmentedFSWal{
		buff:     make([]string, 0, buffLen),
		buffSize: 0,
		reader:   segmentReader,
		writer:   segmentWriter,
		segment:  segment,
		conf:     conf,
		logger:   logger,
		ticker:   time.NewTicker(time.Second),
		mu:       new(sync.Mutex),
	}

	go func() {
		wal.autoFlush()
	}()

	return wal, nil
}

func (s *SegmentedFSWal) ForEach(f func(string) error) error {
	return s.reader.ForEach(f)
}

func (s *SegmentedFSWal) Append(queryString string) error {
	if int64(len(queryString)) > s.conf.GetMaxSegmentSize() {
		return errors.New("query is larger than max segment size")
	}

	s.mu.Lock()
	s.buff = append(s.buff, queryString)
	s.buffSize += len(queryString)

	if len(s.buff) >= s.conf.FlushingBatchSize {
		s.mu.Unlock()
		err := s.flush()
		if err != nil {
			return err
		}
		s.buff = s.buff[:0]
	} else {
		s.mu.Unlock()
	}

	return nil
}

func (s *SegmentedFSWal) Close() error {
	s.ticker.Stop()

	err := s.flush()
	if err != nil {
		s.logger.Error("last flush before closing has been failed", zap.Error(err))
		return err
	}

	return s.writer.Close()
}

func (s *SegmentedFSWal) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.writer.Write(s.buff)
	if err != nil {
		return err
	}

	s.buff = s.buff[:0]

	return nil
}

func (s *SegmentedFSWal) autoFlush() {
	for {
		select {
		case <-s.ticker.C:
			if s.buffSize > 0 {
				err := s.flush()
				if err != nil {
					s.logger.Error("auto flush been failed", zap.Error(err))
				}
			}
		}
	}
}
