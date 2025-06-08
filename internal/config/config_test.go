//go:build unit

package config_test

import (
	"concurrency_hw/internal/config"
	"testing"
	"time"
)

func TestParseRequestSizeInBytes(t *testing.T) {
	tests := []struct {
		name    string
		size    string
		want    int64
		wantErr bool
	}{
		{
			name:    "Valid bytes",
			size:    "1024b",
			want:    1024,
			wantErr: false,
		},
		{
			name:    "Valid kilobytes",
			size:    "4KB",
			want:    4096,
			wantErr: false,
		},
		{
			name:    "Valid megabytes",
			size:    "2MB",
			want:    2097152,
			wantErr: false,
		},
		{
			name:    "Invalid format",
			size:    "invalid",
			want:    0,
			wantErr: true,
		},
		{
			name:    "Invalid number",
			size:    "abcKB",
			want:    0,
			wantErr: true,
		},
		{
			name:    "Invalid unit",
			size:    "1024GB",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.ParseSizeInBytes(tt.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSizeInBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSizeInBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWalConfig_GetMaxSegmentSize(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.WalConfig
		want int64
	}{
		{
			name: "Valid bytes",
			cfg: &config.WalConfig{
				MaxSegmentSize: "1024b",
			},
			want: 1024,
		},
		{
			name: "Valid kilobytes",
			cfg: &config.WalConfig{
				MaxSegmentSize: "4KB",
			},
			want: 4096,
		},
		{
			name: "Valid megabytes",
			cfg: &config.WalConfig{
				MaxSegmentSize: "2MB",
			},
			want: 2097152,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetMaxSegmentSize()
			if got != tt.want {
				t.Errorf("WalConfig.GetMaxSegmentSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWalConfig_GetMaxSegmentSize_Caching(t *testing.T) {
	cfg := &config.WalConfig{
		MaxSegmentSize: "1KB",
	}

	first := cfg.GetMaxSegmentSize()
	if first != 1024 {
		t.Errorf("First call: expected 1024, got %v", first)
	}

	// Изменяем строку MaxSegmentSize, но кеш должен сохраниться
	cfg.MaxSegmentSize = "2KB"

	second := cfg.GetMaxSegmentSize()
	if second != 1024 {
		t.Errorf("Second call: expected cached value 1024, got %v", second)
	}

	// Третий вызов тоже должен вернуть кешированное значение
	third := cfg.GetMaxSegmentSize()
	if third != 1024 {
		t.Errorf("Third call: expected cached value 1024, got %v", third)
	}
}

func TestReplicationConfig_Defaults(t *testing.T) {
	tests := []struct {
		name          string
		replicaType   config.ReplicaType
		masterAddress string
		syncInterval  time.Duration
		wantType      config.ReplicaType
		wantAddress   string
		wantInterval  time.Duration
	}{
		{
			name:          "Master configuration",
			replicaType:   config.Master,
			masterAddress: "127.0.0.1:3223",
			syncInterval:  time.Second,
			wantType:      config.Master,
			wantAddress:   "127.0.0.1:3223",
			wantInterval:  time.Second,
		},
		{
			name:          "Slave configuration",
			replicaType:   config.Slave,
			masterAddress: "192.168.1.10:3223",
			syncInterval:  5 * time.Second,
			wantType:      config.Slave,
			wantAddress:   "192.168.1.10:3223",
			wantInterval:  5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ReplicationConfig{
				ReplicaType:   tt.replicaType,
				MasterAddress: tt.masterAddress,
				SyncInterval:  tt.syncInterval,
			}

			if cfg.ReplicaType != tt.wantType {
				t.Errorf("ReplicationConfig.ReplicaType = %v, want %v", cfg.ReplicaType, tt.wantType)
			}
			if cfg.MasterAddress != tt.wantAddress {
				t.Errorf("ReplicationConfig.MasterAddress = %v, want %v", cfg.MasterAddress, tt.wantAddress)
			}
			if cfg.SyncInterval != tt.wantInterval {
				t.Errorf("ReplicationConfig.SyncInterval = %v, want %v", cfg.SyncInterval, tt.wantInterval)
			}
		})
	}
}

func TestAppConfig_ReplicatorNetworkConfig(t *testing.T) {
	tests := []struct {
		name        string
		appConfig   *config.AppConfig
		wantAddress string
		wantMaxConn int
		wantMsgSize string
		wantTimeout time.Duration
	}{
		{
			name: "Basic replicator network config",
			appConfig: &config.AppConfig{
				NetworkConfig: &config.NetworkConfig{
					Address:        "127.0.0.1:3223",
					MaxConnections: 100,
					MaxMessageSize: "4KB",
					IdleTimeout:    5 * time.Minute,
				},
				ReplicationConfig: &config.ReplicationConfig{
					MasterAddress: "192.168.1.10:3333",
				},
			},
			wantAddress: "192.168.1.10:3333",
			wantMaxConn: 100,
			wantMsgSize: "4KB",
			wantTimeout: 5 * time.Minute,
		},
		{
			name: "Different master address",
			appConfig: &config.AppConfig{
				NetworkConfig: &config.NetworkConfig{
					Address:        "localhost:8080",
					MaxConnections: 50,
					MaxMessageSize: "8KB",
					IdleTimeout:    10 * time.Minute,
				},
				ReplicationConfig: &config.ReplicationConfig{
					MasterAddress: "master.example.com:9999",
				},
			},
			wantAddress: "master.example.com:9999",
			wantMaxConn: 50,
			wantMsgSize: "8KB",
			wantTimeout: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicatorConfig := tt.appConfig.ReplicatorNetworkConfig()

			if replicatorConfig.Address != tt.wantAddress {
				t.Errorf("ReplicatorNetworkConfig().Address = %v, want %v", replicatorConfig.Address, tt.wantAddress)
			}
			if replicatorConfig.MaxConnections != tt.wantMaxConn {
				t.Errorf("ReplicatorNetworkConfig().MaxConnections = %v, want %v", replicatorConfig.MaxConnections, tt.wantMaxConn)
			}
			if replicatorConfig.MaxMessageSize != tt.wantMsgSize {
				t.Errorf("ReplicatorNetworkConfig().MaxMessageSize = %v, want %v", replicatorConfig.MaxMessageSize, tt.wantMsgSize)
			}
			if replicatorConfig.IdleTimeout != tt.wantTimeout {
				t.Errorf("ReplicatorNetworkConfig().IdleTimeout = %v, want %v", replicatorConfig.IdleTimeout, tt.wantTimeout)
			}

			// Проверяем, что изменения в реplicator config не влияют на оригинальную сетевую конфигурацию
			if tt.appConfig.NetworkConfig.Address == replicatorConfig.Address && tt.appConfig.ReplicationConfig.MasterAddress != tt.appConfig.NetworkConfig.Address {
				t.Errorf("Original NetworkConfig should not be modified")
			}
		})
	}
}

func TestEngineConfig_PartitionsCount(t *testing.T) {
	tests := []struct {
		name           string
		engineConfig   *config.EngineConfig
		wantType       string
		wantStartSize  int
		wantPartitions int
	}{
		{
			name: "Default partitions count",
			engineConfig: &config.EngineConfig{
				Type:            "in_memory",
				StartSize:       1000,
				PartitionsCount: 5,
			},
			wantType:       "in_memory",
			wantStartSize:  1000,
			wantPartitions: 5,
		},
		{
			name: "Custom partitions count",
			engineConfig: &config.EngineConfig{
				Type:            "in_memory",
				StartSize:       2000,
				PartitionsCount: 10,
			},
			wantType:       "in_memory",
			wantStartSize:  2000,
			wantPartitions: 10,
		},
		{
			name: "Single partition",
			engineConfig: &config.EngineConfig{
				Type:            "in_memory",
				StartSize:       500,
				PartitionsCount: 1,
			},
			wantType:       "in_memory",
			wantStartSize:  500,
			wantPartitions: 1,
		},
		{
			name: "No partitions (disabled)",
			engineConfig: &config.EngineConfig{
				Type:            "in_memory",
				StartSize:       1500,
				PartitionsCount: 0,
			},
			wantType:       "in_memory",
			wantStartSize:  1500,
			wantPartitions: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.engineConfig.Type != tt.wantType {
				t.Errorf("EngineConfig.Type = %v, want %v", tt.engineConfig.Type, tt.wantType)
			}
			if tt.engineConfig.StartSize != tt.wantStartSize {
				t.Errorf("EngineConfig.StartSize = %v, want %v", tt.engineConfig.StartSize, tt.wantStartSize)
			}
			if tt.engineConfig.PartitionsCount != tt.wantPartitions {
				t.Errorf("EngineConfig.PartitionsCount = %v, want %v", tt.engineConfig.PartitionsCount, tt.wantPartitions)
			}
		})
	}
}

func TestAppConfig_StructureIntegrity(t *testing.T) {
	appConfig := &config.AppConfig{
		EngineConfig: &config.EngineConfig{
			Type:            "in_memory",
			StartSize:       1000,
			PartitionsCount: 5,
		},
		NetworkConfig: &config.NetworkConfig{
			Address:        "127.0.0.1:3223",
			MaxConnections: 100,
			MaxMessageSize: "4KB",
			IdleTimeout:    5 * time.Minute,
		},
		LoggingConfig: &config.LoggingConfig{
			Level:  "info",
			Output: "/tmp/output.log",
		},
		WalConfig: &config.WalConfig{
			FlushingBatchSize:    100,
			FlushingBatchTimeout: 10 * time.Millisecond,
			MaxSegmentSize:       "1KB",
			DataDirectory:        "/tmp/data",
		},
		ReplicationConfig: &config.ReplicationConfig{
			ReplicaType:   config.Master,
			MasterAddress: "127.0.0.1:3223",
			SyncInterval:  time.Second,
		},
	}

	// Проверяем, что все поля установлены корректно
	if appConfig.EngineConfig == nil {
		t.Error("EngineConfig should not be nil")
	}
	if appConfig.NetworkConfig == nil {
		t.Error("NetworkConfig should not be nil")
	}
	if appConfig.LoggingConfig == nil {
		t.Error("LoggingConfig should not be nil")
	}
	if appConfig.WalConfig == nil {
		t.Error("WalConfig should not be nil")
	}
	if appConfig.ReplicationConfig == nil {
		t.Error("ReplicationConfig should not be nil")
	}

	// Проверяем новые поля
	if appConfig.EngineConfig.PartitionsCount != 5 {
		t.Errorf("EngineConfig.PartitionsCount = %v, want 5", appConfig.EngineConfig.PartitionsCount)
	}
	if appConfig.ReplicationConfig.ReplicaType != config.Master {
		t.Errorf("ReplicationConfig.ReplicaType = %v, want %v", appConfig.ReplicationConfig.ReplicaType, config.Master)
	}
}
