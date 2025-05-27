package config

import (
	"testing"
)

func TestNetworkConfig_ParseRequestSizeInBytes(t *testing.T) {
	tests := []struct {
		name    string
		size    string
		want    int
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
			cfg := &NetworkConfig{
				MaxMessageSize: tt.size,
			}
			got, err := cfg.ParseRequestSizeInBytes()
			if (err != nil) != tt.wantErr {
				t.Errorf("NetworkConfig.ParseRequestSizeInBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("NetworkConfig.ParseRequestSizeInBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
