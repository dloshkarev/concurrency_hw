package wal

import (
	"bufio"
	"concurrency_hw/internal/config"
	"github.com/facette/natsort"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type SegmentReader interface {
	Open() (*Segment, error)
	ForEach(func(string) error) error
}

type StringSegmentReader struct {
	conf *config.WalConfig
}

func NewStringSegmentReader(conf *config.WalConfig) *StringSegmentReader {
	return &StringSegmentReader{conf: conf}
}

func (r *StringSegmentReader) Open() (*Segment, error) {
	err := createDirIfNotExists(r.conf.DataDirectory)
	if err != nil {
		return nil, err
	}

	lastSegmentPath, err := findLastSegmentPath(r.conf.DataDirectory)
	if err != nil {
		return nil, err
	}

	err = createSegmentFileIfNotExists(lastSegmentPath)
	if err != nil {
		return nil, err
	}

	segmentNum, err := getSegmentNum(lastSegmentPath)
	if err != nil {
		return nil, err
	}

	segmentFile, err := os.OpenFile(lastSegmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fileStat, err := segmentFile.Stat()
	if err != nil {
		return nil, err
	}

	segmentSize := fileStat.Size()

	return &Segment{
		segmentNum:     segmentNum,
		file:           segmentFile,
		size:           segmentSize,
		maxSegmentSize: r.conf.GetMaxSegmentSize(),
	}, nil
}

func (r *StringSegmentReader) ForEach(f func(string) error) error {
	segmentPaths, err := findSortedSegments(r.conf.DataDirectory)
	if err != nil {
		return err
	}

	for _, segmentPath := range segmentPaths {
		file, err := os.Open(segmentPath)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			queryString := scanner.Text()
			err = f(queryString)
			if err != nil {
				return err
			}
		}

		return file.Close()
	}

	return nil
}

func getSegmentNum(filePath string) (int, error) {
	filename := filepath.Base(filePath)
	ext := filepath.Ext(filename)
	withoutExt := strings.TrimSuffix(filename, ext)
	num, err := strconv.Atoi(withoutExt)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func findSortedSegments(dir string) ([]string, error) {
	filenames := make([]string, 0)
	segments, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, segment := range segments {
		fullPath := filepath.Join(dir, segment.Name())
		absPath, _ := filepath.Abs(fullPath)
		filenames = append(filenames, absPath)
	}

	natsort.Sort(filenames)

	return filenames, nil
}

func findLastSegmentPath(dir string) (string, error) {
	filenames, err := findSortedSegments(dir)
	if err != nil {
		return "", err
	}

	if len(filenames) == 0 {
		firstSegment, err := filepath.Abs(filepath.Join(dir, "0"))
		if err != nil {
			return "", err
		}
		return firstSegment, nil
	}

	lastSegmentPath := filenames[len(filenames)-1]

	return lastSegmentPath, nil
}
