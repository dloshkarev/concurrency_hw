package main

import (
	"bufio"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"
	"log"
	"net"
	"os"
)

const (
	ReadBufferSize = 1024
)

// Клиент максимально колхозный, т.к предполагается что у нашей БД может быть множество клиентов и качество их гарантировать нельзя
// Поэтому тут нет никаких ограничений и проверок на что либо
func main() {
	logger, _ := zap.NewProduction()

	address := flag.String("address", "localhost:3223", "tcp server address")

	client, err := NewTCPClient(*address)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Disconnect(); err != nil {
			logger.Fatal("error on disconnect", zap.Error(err))
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		queryString, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Error("server connection closed",
					zap.Error(err),
				)
			} else {
				logger.Error("cannot read query",
					zap.String("query", queryString),
					zap.Error(err),
				)
			}

			return
		}

		response, err := client.Execute(queryString)
		if err != nil {
			logger.Error("cannot execute query",
				zap.String("query", queryString),
				zap.Error(err),
			)

			return
		}

		fmt.Println(string(response))
	}
}

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
