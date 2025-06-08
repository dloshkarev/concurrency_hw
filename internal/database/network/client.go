package network

import (
	"encoding/binary"
	"fmt"
	"net"
)

type TCPClient struct {
	conn    net.Conn
	address string
}

func NewTCPClient(address string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return &TCPClient{
		conn:    conn,
		address: address,
	}, nil
}

func (c *TCPClient) Execute(request []byte) ([]byte, error) {
	// Отправляем длину запроса и сам запрос
	err := binary.Write(c.conn, binary.BigEndian, uint32(len(request)))
	if err != nil {
		return nil, fmt.Errorf("cannot send request length to server: %w", err)
	}

	_, err = c.conn.Write(request)
	if err != nil {
		return nil, fmt.Errorf("cannot send request to server: %w", err)
	}

	// Читаем длину ответа
	var responseLength uint32
	err = binary.Read(c.conn, binary.BigEndian, &responseLength)
	if err != nil {
		return nil, fmt.Errorf("cannot read response length: %w", err)
	}

	// Читаем ответ фиксированной длины
	response := make([]byte, responseLength)
	totalRead := 0
	for totalRead < int(responseLength) {
		n, err := c.conn.Read(response[totalRead:])
		if err != nil {
			return nil, fmt.Errorf("cannot read response: %w", err)
		}
		totalRead += n
	}

	return response, nil
}

func (c *TCPClient) GetAddress() string {
	return c.address
}

func (c *TCPClient) Disconnect() error {
	if err := c.conn.Close(); err != nil {
		return err
	}

	return nil
}
