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

type WalStatus struct {
	SegmentNum  int
	SegmentLine int
}

type Wal interface {
	ForEach(func(string) error) error
	Append(string) error
	GetUpdates(status *WalStatus) ([]string, error)
	GetWalStatus() *WalStatus
	Close() error
}

type Segment struct {
	segmentNum     int
	file           *os.File
	length         int
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
	skipTick bool
	ticker   *time.Ticker
	mu       *sync.Mutex
}

func NewSegmentedFSWal(
	conf *config.WalConfig,
	logger *zap.Logger,
	lastSegment *Segment,
	segmentReader SegmentReader,
	segmentWriter SegmentWriter,
) (*SegmentedFSWal, error) {
	buffLen := int(1.1 * float64(conf.FlushingBatchSize))

	wal := &SegmentedFSWal{
		buff:     make([]string, 0, buffLen),
		buffSize: 0,
		reader:   segmentReader,
		writer:   segmentWriter,
		segment:  lastSegment,
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

		s.mu.Lock()
		s.skipTick = true
		s.mu.Unlock()

		if err != nil {
			return err
		}
	} else {
		s.mu.Unlock()
	}

	return nil
}

func (s *SegmentedFSWal) GetUpdates(status *WalStatus) ([]string, error) {
	if s.segment.segmentNum < status.SegmentNum || s.segment.length > status.SegmentLine {
		return s.reader.ReadFrom(status.SegmentNum, status.SegmentLine)
	}

	return nil, nil
}

func (s *SegmentedFSWal) GetWalStatus() *WalStatus {
	return &WalStatus{
		SegmentNum:  s.segment.segmentNum,
		SegmentLine: s.segment.length,
	}
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
	for range s.ticker.C {
		s.mu.Lock()
		if s.skipTick {
			s.skipTick = false
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		if s.buffSize > 0 {
			err := s.flush()
			if err != nil {
				s.logger.Error("auto flush has been failed", zap.Error(err))
			}
		}
	}
}
