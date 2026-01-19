package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ListenAddr != ":9000" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":9000")
	}
	if cfg.BackendType != "disk" {
		t.Errorf("BackendType = %q, want %q", cfg.BackendType, "disk")
	}
	if cfg.DataDir != "/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/data")
	}
	if cfg.LocalStackEndpoint != "http://localhost:4566" {
		t.Errorf("LocalStackEndpoint = %q, want %q", cfg.LocalStackEndpoint, "http://localhost:4566")
	}
	if cfg.AWSRegion != "us-east-1" {
		t.Errorf("AWSRegion = %q, want %q", cfg.AWSRegion, "us-east-1")
	}
	if cfg.BucketMappings == nil {
		t.Error("BucketMappings should not be nil")
	}
	if cfg.InitBuckets == nil {
		t.Error("InitBuckets should not be nil")
	}
}

func TestLoadConfig_BackendType(t *testing.T) {
	// Clear all s3lazy env vars first
	clearS3LazyEnvVars(t)

	tests := []struct {
		name     string
		envValue string
		want     string
	}{
		{"default when unset", "", "disk"},
		{"disk explicitly", "disk", "disk"},
		{"memory", "memory", "memory"},
		{"localstack", "localstack", "localstack"},
		{"custom value", "custom", "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearS3LazyEnvVars(t)

			if tt.envValue != "" {
				t.Setenv("S3LAZY_BACKEND", tt.envValue)
			}

			cfg := LoadConfig()

			if cfg.BackendType != tt.want {
				t.Errorf("BackendType = %q, want %q", cfg.BackendType, tt.want)
			}
		})
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	clearS3LazyEnvVars(t)

	t.Setenv("S3LAZY_LISTEN_ADDR", ":8080")
	t.Setenv("S3LAZY_BACKEND", "localstack")
	t.Setenv("S3LAZY_DATA_DIR", "/custom/data")
	t.Setenv("S3LAZY_LOCALSTACK_ENDPOINT", "http://localstack:4566")
	t.Setenv("S3LAZY_AWS_REGION", "eu-west-1")

	cfg := LoadConfig()

	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8080")
	}
	if cfg.BackendType != "localstack" {
		t.Errorf("BackendType = %q, want %q", cfg.BackendType, "localstack")
	}
	if cfg.DataDir != "/custom/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/custom/data")
	}
	if cfg.LocalStackEndpoint != "http://localstack:4566" {
		t.Errorf("LocalStackEndpoint = %q, want %q", cfg.LocalStackEndpoint, "http://localstack:4566")
	}
	if cfg.AWSRegion != "eu-west-1" {
		t.Errorf("AWSRegion = %q, want %q", cfg.AWSRegion, "eu-west-1")
	}
}

func TestLoadConfig_AWSRegionFallback(t *testing.T) {
	clearS3LazyEnvVars(t)

	// AWS_REGION should be used if S3LAZY_AWS_REGION is not set
	t.Setenv("AWS_REGION", "ap-southeast-1")

	cfg := LoadConfig()

	if cfg.AWSRegion != "ap-southeast-1" {
		t.Errorf("AWSRegion = %q, want %q", cfg.AWSRegion, "ap-southeast-1")
	}

	// But S3LAZY_AWS_REGION takes precedence
	t.Setenv("S3LAZY_AWS_REGION", "us-west-2")

	cfg = LoadConfig()

	if cfg.AWSRegion != "us-west-2" {
		t.Errorf("AWSRegion = %q, want %q (S3LAZY_AWS_REGION should take precedence)", cfg.AWSRegion, "us-west-2")
	}
}

func TestLoadConfig_InitBucketsParsing(t *testing.T) {
	clearS3LazyEnvVars(t)

	tests := []struct {
		name     string
		envValue string
		want     []string
	}{
		{"empty", "", []string{}},
		{"single bucket", "bucket1", []string{"bucket1"}},
		{"multiple buckets", "bucket1,bucket2,bucket3", []string{"bucket1", "bucket2", "bucket3"}},
		{"with spaces", " bucket1 , bucket2 , bucket3 ", []string{"bucket1", "bucket2", "bucket3"}},
		{"empty entries ignored", "bucket1,,bucket2", []string{"bucket1", "bucket2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearS3LazyEnvVars(t)

			if tt.envValue != "" {
				t.Setenv("S3LAZY_INIT_BUCKETS", tt.envValue)
			}

			cfg := LoadConfig()

			if len(cfg.InitBuckets) != len(tt.want) {
				t.Errorf("InitBuckets length = %d, want %d", len(cfg.InitBuckets), len(tt.want))
				return
			}

			for i, bucket := range cfg.InitBuckets {
				if bucket != tt.want[i] {
					t.Errorf("InitBuckets[%d] = %q, want %q", i, bucket, tt.want[i])
				}
			}
		})
	}
}

func TestLoadConfig_BucketMapParsing(t *testing.T) {
	clearS3LazyEnvVars(t)

	tests := []struct {
		name     string
		envValue string
		want     map[string]string
	}{
		{"empty", "", map[string]string{}},
		{"single mapping", "local:aws", map[string]string{"local": "aws"}},
		{"multiple mappings", "local1:aws1,local2:aws2", map[string]string{"local1": "aws1", "local2": "aws2"}},
		{"with spaces", " local1 : aws1 , local2 : aws2 ", map[string]string{"local1": "aws1", "local2": "aws2"}},
		{"invalid entry ignored", "local1:aws1,invalid,local2:aws2", map[string]string{"local1": "aws1", "local2": "aws2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearS3LazyEnvVars(t)

			if tt.envValue != "" {
				t.Setenv("S3LAZY_BUCKET_MAP", tt.envValue)
			}

			cfg := LoadConfig()

			if len(cfg.BucketMappings) != len(tt.want) {
				t.Errorf("BucketMappings length = %d, want %d", len(cfg.BucketMappings), len(tt.want))
				return
			}

			for k, v := range tt.want {
				if cfg.BucketMappings[k] != v {
					t.Errorf("BucketMappings[%q] = %q, want %q", k, cfg.BucketMappings[k], v)
				}
			}
		})
	}
}

func TestLoadConfig_YAMLFile(t *testing.T) {
	clearS3LazyEnvVars(t)

	// Create a temporary YAML config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
listen_addr: ":8888"
backend_type: "localstack"
data_dir: "/yaml/data"
localstack_endpoint: "http://yaml-localstack:4566"
aws_region: "eu-central-1"
init_buckets:
  - "yaml-bucket-1"
  - "yaml-bucket-2"
bucket_mappings:
  yaml-local: "yaml-aws"
`

	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Setenv("S3LAZY_CONFIG_FILE", configPath)

	cfg := LoadConfig()

	if cfg.ListenAddr != ":8888" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8888")
	}
	if cfg.BackendType != "localstack" {
		t.Errorf("BackendType = %q, want %q", cfg.BackendType, "localstack")
	}
	if cfg.DataDir != "/yaml/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/yaml/data")
	}
	if cfg.LocalStackEndpoint != "http://yaml-localstack:4566" {
		t.Errorf("LocalStackEndpoint = %q, want %q", cfg.LocalStackEndpoint, "http://yaml-localstack:4566")
	}
	if cfg.AWSRegion != "eu-central-1" {
		t.Errorf("AWSRegion = %q, want %q", cfg.AWSRegion, "eu-central-1")
	}
	if len(cfg.InitBuckets) != 2 || cfg.InitBuckets[0] != "yaml-bucket-1" {
		t.Errorf("InitBuckets = %v, want [yaml-bucket-1 yaml-bucket-2]", cfg.InitBuckets)
	}
	if cfg.BucketMappings["yaml-local"] != "yaml-aws" {
		t.Errorf("BucketMappings[yaml-local] = %q, want %q", cfg.BucketMappings["yaml-local"], "yaml-aws")
	}
}

func TestLoadConfig_EnvOverridesYAML(t *testing.T) {
	clearS3LazyEnvVars(t)

	// Create a temporary YAML config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
listen_addr: ":8888"
backend_type: "disk"
`

	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Setenv("S3LAZY_CONFIG_FILE", configPath)
	// Env var should override YAML
	t.Setenv("S3LAZY_BACKEND", "localstack")

	cfg := LoadConfig()

	// YAML value
	if cfg.ListenAddr != ":8888" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8888")
	}
	// Env var override
	if cfg.BackendType != "localstack" {
		t.Errorf("BackendType = %q, want %q (env should override yaml)", cfg.BackendType, "localstack")
	}
}

func TestLoadConfig_InvalidYAMLFile(t *testing.T) {
	clearS3LazyEnvVars(t)

	// Non-existent file - should use defaults
	t.Setenv("S3LAZY_CONFIG_FILE", "/nonexistent/config.yaml")

	cfg := LoadConfig()

	// Should fall back to defaults
	if cfg.BackendType != "disk" {
		t.Errorf("BackendType = %q, want %q (should use default when file not found)", cfg.BackendType, "disk")
	}
}

func TestLoadConfig_MalformedYAML(t *testing.T) {
	clearS3LazyEnvVars(t)

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	// Invalid YAML - tabs are not allowed in YAML
	malformedYAML := "backend_type: localstack\n\t\tinvalid indentation"

	if err := os.WriteFile(configPath, []byte(malformedYAML), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Setenv("S3LAZY_CONFIG_FILE", configPath)

	cfg := LoadConfig()

	// Should fall back to defaults when YAML parse fails
	if cfg.BackendType != "disk" {
		t.Errorf("BackendType = %q, want %q (should use default on parse error)", cfg.BackendType, "disk")
	}
}

func TestLoadConfig_YAMLWrongFieldName(t *testing.T) {
	clearS3LazyEnvVars(t)

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	// Common mistake: using camelCase instead of snake_case
	yamlContent := `backendType: localstack`

	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Setenv("S3LAZY_CONFIG_FILE", configPath)

	cfg := LoadConfig()

	// backendType (camelCase) is ignored, default is used
	if cfg.BackendType != "disk" {
		t.Errorf("BackendType = %q, want %q (camelCase field should be ignored)", cfg.BackendType, "disk")
	}
}

func TestParseCommaSeparated(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"", []string{}},
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{" a , b , c ", []string{"a", "b", "c"}},
		{"a,,b", []string{"a", "b"}},
		{",,,", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseCommaSeparated(tt.input)

			if len(got) != len(tt.want) {
				t.Errorf("parseCommaSeparated(%q) length = %d, want %d", tt.input, len(got), len(tt.want))
				return
			}

			for i, v := range got {
				if v != tt.want[i] {
					t.Errorf("parseCommaSeparated(%q)[%d] = %q, want %q", tt.input, i, v, tt.want[i])
				}
			}
		})
	}
}

// clearS3LazyEnvVars clears all S3LAZY_* environment variables for test isolation
func clearS3LazyEnvVars(t *testing.T) {
	t.Helper()
	envVars := []string{
		"S3LAZY_LISTEN_ADDR",
		"S3LAZY_BACKEND",
		"S3LAZY_DATA_DIR",
		"S3LAZY_LOCALSTACK_ENDPOINT",
		"S3LAZY_AWS_REGION",
		"S3LAZY_CONFIG_FILE",
		"S3LAZY_INIT_BUCKETS",
		"S3LAZY_BUCKET_MAP",
		"AWS_REGION",
	}
	for _, env := range envVars {
		t.Setenv(env, "")
	}
}
