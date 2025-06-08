package replicator

import (
	"bytes"
	"encoding/gob"
)

type Request struct {
	LastSegmentNum  int
	LastSegmentLine int
}

type Response struct {
	Queries []string
}

func Encode(obj any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, target any) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(target)
}
