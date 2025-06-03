//go:build unit

package config_test

import (
	"concurrency_hw/internal/config"
	"testing"
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
