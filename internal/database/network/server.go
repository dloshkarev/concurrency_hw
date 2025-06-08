package network

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/primitive"
	"context"
	"encoding/binary"
	"net"
	"time"

	"go.uber.org/zap"
)

type TCPServer struct {
	logger           *zap.Logger
	conf             *config.NetworkConfig
	requestHandler   func([]byte) ([]byte, error)
	semaphore        *primitive.Semaphore
	requestBytesSize int64
}

func NewTCPServer(
	logger *zap.Logger,
	conf *config.NetworkConfig,
	semaphore *primitive.Semaphore,
	requestHandler func([]byte) ([]byte, error),
) (*TCPServer, error) {
	requestBytesSize, err := config.ParseSizeInBytes(conf.MaxMessageSize)
	if err != nil {
		return nil, err
	}

	return &TCPServer{
		logger:           logger,
		conf:             conf,
		requestBytesSize: requestBytesSize,
		requestHandler:   requestHandler,
		semaphore:        semaphore,
	}, nil
}

func (s *TCPServer) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.conf.Address)
	if err != nil {
		return err
	}
	defer func() {
		if err := listener.Close(); err != nil {
			s.logger.Error("failed to close listener", zap.Error(err))
		}
	}()

	s.logger.Info("listening on port" + listener.Addr().String())

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("shutting down tcp server")
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Error("failed to accept connection", zap.Error(err))
				continue
			}

			s.semaphore.Acquire()
			go func() {
				defer s.semaphore.Release()
				s.handleConnection(ctx, conn)
			}()
		}

	}
}

func (s *TCPServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("captured panic", zap.Any("panic", r))
		}

		s.CloseConnection(conn)
	}()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("closing tcp connection")
			return
		default:
			if s.conf.IdleTimeout > 0 {
				if err := conn.SetReadDeadline(time.Now().Add(s.conf.IdleTimeout)); err != nil {
					s.logger.Error("failed to set read deadline", zap.Error(err))
				}
			}

			// Читаем длину запроса
			var requestLength uint32
			err := binary.Read(conn, binary.BigEndian, &requestLength)
			if err != nil {
				s.logger.Error("failed to read request length", zap.Error(err))
				return
			}

			// Читаем запрос фиксированной длины
			request := make([]byte, requestLength)
			totalRead := 0
			for totalRead < int(requestLength) {
				n, err := conn.Read(request[totalRead:])
				if err != nil {
					s.logger.Error("failed to read request", zap.Error(err))
					return
				}
				totalRead += n
			}

			response, err := s.requestHandler(request)

			if err != nil {
				s.logger.Error("failed to handle request",
					zap.ByteString("request", request),
					zap.ByteString("response", response),
					zap.Error(err),
				)
			}

			s.response(conn, response)
		}
	}
}

func (s *TCPServer) CloseConnection(conn net.Conn) {
	if err := conn.Close(); err != nil {
		s.logger.Error("failed to close connection", zap.Error(err))
	}
	s.semaphore.Release()
}

func (s *TCPServer) response(conn net.Conn, response []byte) {
	// Отправляем длину ответа и сам ответ
	err := binary.Write(conn, binary.BigEndian, uint32(len(response)))
	if err != nil {
		s.logger.Error("failed to write response length", zap.Error(err))
		return
	}

	if _, err := conn.Write(response); err != nil {
		s.logger.Error("failed to write response",
			zap.ByteString("response", response),
			zap.Error(err),
		)
	}
}
