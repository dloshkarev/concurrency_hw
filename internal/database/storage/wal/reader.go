package wal

import (
	"bufio"
	"concurrency_hw/internal/config"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/facette/natsort"
)

type SegmentReader interface {
	ForEach(func(string) error) error
	ReadFrom(segmentNum int, segmentLine int) ([]string, error)
}

type StringSegmentReader struct {
	conf *config.WalConfig
}

func NewStringSegmentReader(conf *config.WalConfig) (*StringSegmentReader, *Segment, error) {
	segment, err := openSegment(conf)
	if err != nil {
		return nil, nil, err
	}
	return &StringSegmentReader{conf: conf}, segment, nil
}

func openSegment(conf *config.WalConfig) (*Segment, error) {
	err := createDirIfNotExists(conf.DataDirectory)
	if err != nil {
		return nil, err
	}

	lastSegmentPath, err := findLastSegmentPath(conf.DataDirectory)
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

	segmentFile, err := os.OpenFile(lastSegmentPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fileStat, err := segmentFile.Stat()
	if err != nil {
		return nil, err
	}

	segmentSize := fileStat.Size()
	linesCount, err := countLines(segmentFile)
	if err != nil {
		return nil, err
	}

	return &Segment{
		segmentNum:     segmentNum,
		file:           segmentFile,
		size:           segmentSize,
		length:         linesCount,
		maxSegmentSize: conf.GetMaxSegmentSize(),
	}, nil
}

func (r *StringSegmentReader) ForEach(f func(string) error) error {
	segmentPaths, err := findSortedSegments(r.conf.DataDirectory)
	if err != nil {
		return err
	}

	for _, segmentPath := range segmentPaths {
		err := r.ForEachInSegment(segmentPath, f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *StringSegmentReader) ReadFrom(segmentNum int, segmentLine int) ([]string, error) {
	filenames, err := findSortedSegments(r.conf.DataDirectory)
	if err != nil {
		return nil, err
	}

	queries := make([]string, 0)

	for _, segmentFile := range filenames {
		currentSegmentNum, err := getSegmentNum(segmentFile)
		if err != nil {
			return nil, err
		}

		if currentSegmentNum >= segmentNum {
			var line = 1
			err := r.ForEachInSegment(segmentFile, func(query string) error {
				if line > segmentLine {
					queries = append(queries, segmentFile)
				}
				line++
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return queries, nil
}

func countLines(file *os.File) (int, error) {
	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}

func (r *StringSegmentReader) ForEachInSegment(segmentPath string, f func(string) error) error {
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

	err = file.Close()
	if err != nil {
		return err
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
