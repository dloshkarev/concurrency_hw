package main

import (
	"concurrency_hw/internal/config"
	"concurrency_hw/internal/database"
	"concurrency_hw/internal/database/compute"
	"concurrency_hw/internal/database/network"
	"concurrency_hw/internal/database/storage/engine/mem"
	"context"
	"errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	errUnknownLoggerLevel = errors.New("unknown logger level")
)

func main() {
	conf := config.Load()

	logger := createLogger(conf.LoggingConfig)
	defer func() {
		_ = logger.Sync()
	}()

	db := createDatabase(logger, conf.EngineConfig)

	server, err := network.NewTCPServer(logger, conf.NetworkConfig, db.Execute)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := server.Run(ctx); err != nil {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	shutdown(logger, cancel)
}

func createLogger(conf *config.LoggingConfig) *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var zapLevel = zapcore.InfoLevel

	levelByName := map[string]zapcore.Level{
		"info":  zapcore.InfoLevel,
		"debug": zapcore.DebugLevel,
		"warn":  zapcore.WarnLevel,
		"error": zapcore.ErrorLevel,
	}

	var found bool
	if zapLevel, found = levelByName[conf.Level]; !found {
		log.Fatal(errUnknownLoggerLevel)
	}

	cfg := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		DisableCaller:     false,
		DisableStacktrace: false,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
		OutputPaths: []string{
			"stderr",
			conf.Output,
		},
		ErrorOutputPaths: []string{
			"stderr",
		},
		InitialFields: map[string]interface{}{
			"pid": os.Getpid(),
		},
		Development: false,
		Sampling:    nil,
	}

	return zap.Must(cfg.Build())
}

func createDatabase(logger *zap.Logger, conf *config.EngineConfig) *database.Database {
	parser, err := compute.NewQueryParser(logger)
	if err != nil {
		logger.Fatal("Failed to create query parser", zap.Error(err))
	}

	engine := mem.NewInMemoryEngine(conf.StartSize)

	return database.NewDatabase(parser, engine)
}

func shutdown(logger *zap.Logger, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-sigChan
	logger.Info("shutting down server...")
	cancel()
}
