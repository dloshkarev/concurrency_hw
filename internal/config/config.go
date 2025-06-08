package config

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	Master = ReplicaType("master")
	Slave  = ReplicaType("slave")
)

type ReplicaType string

type AppConfig struct {
	EngineConfig      *EngineConfig      `yaml:"engine"`
	NetworkConfig     *NetworkConfig     `yaml:"network"`
	LoggingConfig     *LoggingConfig     `yaml:"logging"`
	WalConfig         *WalConfig         `yaml:"wal"`
	ReplicationConfig *ReplicationConfig `yaml:"replication"`
}

type EngineConfig struct {
	Type            string `yaml:"type" env-default:"in_memory"`
	StartSize       int    `yaml:"start_size" env-default:"1000"`
	PartitionsCount int    `yaml:"partitions_count" env-default:"5"`
}

type NetworkConfig struct {
	Address        string        `yaml:"address" env-default:"127.0.0.1:3223"`
	MaxConnections int           `yaml:"max_connections" env-default:"100"`
	MaxMessageSize string        `yaml:"max_message_size" env-default:"4KB"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" env-default:"5m"`
}

type LoggingConfig struct {
	Level  string `yaml:"level" env-default:"info"`
	Output string `yaml:"output" env-default:"/wal/output.wal"`
}

type WalConfig struct {
	FlushingBatchSize     int           `yaml:"flushing_batch_size" env-default:"100"`
	FlushingBatchTimeout  time.Duration `yaml:"flushing_batch_timeout" env-default:"10ms"`
	MaxSegmentSize        string        `yaml:"max_segment_size" env-default:"1KB"`
	DataDirectory         string        `yaml:"data_directory" env-default:"/data"`
	maxSegmentSizeInBytes int64
}

type ReplicationConfig struct {
	ReplicaType   ReplicaType   `yaml:"replica_type" env-default:"master"`
	MasterAddress string        `yaml:"master_address" env-default:"127.0.0.1:3223"`
	SyncInterval  time.Duration `yaml:"sync_interval" env-default:"1s"`
}

func (c *AppConfig) ReplicatorNetworkConfig() *NetworkConfig {
	replicatorNetConfig := *c.NetworkConfig
	replicatorNetConfig.Address = c.ReplicationConfig.MasterAddress
	replicatorNetConfig.IdleTimeout = 0
	return &replicatorNetConfig
}

func (c *WalConfig) GetMaxSegmentSize() int64 {
	if c.maxSegmentSizeInBytes > 0 {
		return c.maxSegmentSizeInBytes
	}
	maxSegmentSizeInBytes, err := ParseSizeInBytes(c.MaxSegmentSize)
	if err != nil {
		log.Fatal(err)
	}

	c.maxSegmentSizeInBytes = maxSegmentSizeInBytes

	return maxSegmentSizeInBytes
}

func Load() *AppConfig {
	configPath := os.Getenv("CONDB_CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONDB_CONFIG_PATH is not set")
	}

	return LoadFromPath(configPath)
}

func LoadFromPath(configPath string) *AppConfig {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exist: %s", configPath)
	}

	var cfg AppConfig

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}

func ParseSizeInBytes(val string) (int64, error) {
	rxp := regexp.MustCompile(`(\d+)(b|kb|mb)`)
	matches := rxp.FindStringSubmatch(strings.ToLower(val))

	if len(matches) != 3 {
		return 0, fmt.Errorf("unknown format of max_message_size in bytes: %s", val)
	}

	size, err := strconv.Atoi(matches[1])
	var sizeInBytes int64
	if err != nil {
		return 0, fmt.Errorf("cannot parse size of max_message_size in bytes: %s", val)
	}

	switch matches[2] {
	case "b":
		sizeInBytes = int64(size)
	case "kb":
		sizeInBytes = int64(size) << 10
	case "mb":
		sizeInBytes = int64(size) << 20
	default:
		return 0, fmt.Errorf("cannot dimension of max_message_size in bytes: %s", val)
	}

	return sizeInBytes, nil
}
