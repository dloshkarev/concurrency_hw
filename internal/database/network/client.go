package network

import (
	"fmt"
	"io"
	"net"
)

const (
	ReadBufferSize = 1024
)

type TCPClient struct {
	conn net.Conn
}

func NewTCPClient(address string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return &TCPClient{
		conn: conn,
	}, nil
}

func (c *TCPClient) Execute(queryString string) ([]byte, error) {
	_, err := c.conn.Write([]byte(queryString))
	if err != nil {
		return nil, fmt.Errorf("cannot send query to server: %w", err)
	}

	return readResponse(c.conn)
}

func (c *TCPClient) Disconnect() error {
	if err := c.conn.Close(); err != nil {
		return err
	}

	return nil
}

func readResponse(conn net.Conn) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, ReadBufferSize)

	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err == io.EOF {
				buf = append(buf, tmp[:n]...)
				return buf, nil
			}
			return nil, err
		}
		buf = append(buf, tmp[:n]...)
		if n < ReadBufferSize {
			return buf, nil
		}
	}
}
