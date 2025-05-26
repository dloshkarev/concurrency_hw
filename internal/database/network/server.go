package network

import (
	"concurrency_hw/internal/config"
	"context"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
)

type TCPServer struct {
	logger           *zap.Logger
	conf             *config.NetworkConfig
	requestHandler   func(string) (string, error)
	connections      int
	requestBytesSize int
	mu               *sync.Mutex
}

func NewTCPServer(
	logger *zap.Logger,
	conf *config.NetworkConfig,
	requestHandler func(string) (string, error),
) (*TCPServer, error) {
	requestBytesSize, err := conf.ParseRequestSizeInBytes()
	if err != nil {
		return nil, err
	}

	return &TCPServer{
		logger:           logger,
		conf:             conf,
		mu:               &sync.Mutex{},
		requestBytesSize: requestBytesSize,
		requestHandler:   requestHandler,
		connections:      0,
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

			s.mu.Lock()
			if s.connections < s.conf.MaxConnections {
				s.connections++
				s.mu.Unlock()

				go s.handleConnection(ctx, conn)
			} else {
				s.response(conn, []byte(NoConnectionsAvailable))
				s.mu.Unlock()
			}
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

	request := make([]byte, s.requestBytesSize)

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
			count, err := conn.Read(request)
			if err != nil {
				s.logger.Error("failed to read request", zap.Error(err))
				return
			}

			command := string(request[:count])
			response, err := s.requestHandler(command)

			if err != nil {
				s.logger.Error("failed to handle request",
					zap.String("request", command),
					zap.String("response", response),
					zap.Error(err),
				)
			}

			s.response(conn, []byte(response))
		}
	}
}

func (s *TCPServer) CloseConnection(conn net.Conn) {
	s.mu.Lock()
	if err := conn.Close(); err != nil {
		s.logger.Error("failed to close connection", zap.Error(err))
	}
	s.connections--
	s.mu.Unlock()
}

func (s *TCPServer) response(conn net.Conn, response []byte) {
	if _, err := conn.Write(response); err != nil {
		s.logger.Error("failed to write response",
			zap.ByteString("response", response),
			zap.Error(err),
		)
	}
}
