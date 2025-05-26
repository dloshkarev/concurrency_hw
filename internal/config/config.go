package config

import (
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type AppConfig struct {
	EngineConfig  *EngineConfig  `yaml:"engine"`
	NetworkConfig *NetworkConfig `yaml:"network"`
	LoggingConfig *LoggingConfig `yaml:"logging"`
}

type EngineConfig struct {
	Type      string `yaml:"type" env-default:"in_memory"`
	StartSize int    `yaml:"start_size" env-default:"1000"`
}

type NetworkConfig struct {
	Address        string        `yaml:"address" env-default:"127.0.0.1:3223"`
	MaxConnections int           `yaml:"max_connections" env-default:"100"`
	MaxMessageSize string        `yaml:"max_message_size" env-default:"4KB"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" env-default:"5m"`
}

type LoggingConfig struct {
	Level  string `yaml:"level" env-default:"info"`
	Output string `yaml:"output" env-default:"/log/output.log"`
}

func Load() *AppConfig {
	configPath := os.Getenv("CONDB_CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONDB_CONFIG_PATH is not set")
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exist: %s", configPath)
	}

	var cfg AppConfig

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}

func (c *NetworkConfig) ParseRequestSizeInBytes() (int, error) {
	rxp := regexp.MustCompile(`(\d+)(b|kb|mb)`)
	matches := rxp.FindStringSubmatch(strings.ToLower(c.MaxMessageSize))

	if len(matches) != 3 {
		return 0, fmt.Errorf("unknown format of max_message_size in bytes: %s", c.MaxMessageSize)
	}

	size, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("cannot parse size of max_message_size in bytes: %s", c.MaxMessageSize)
	}

	switch matches[2] {
	case "b":
		return size, nil
	case "kb":
		return size << 10, nil
	case "mb":
		return size << 20, nil
	default:
		return 0, fmt.Errorf("cannot dimension of max_message_size in bytes: %s", c.MaxMessageSize)
	}
}
