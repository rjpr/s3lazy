package main

import (
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for s3lazy
type Config struct {
	// Server settings
	ListenAddr string `yaml:"listen_addr"`

	// Backend selection: "disk", "memory", or "localstack"
	BackendType string `yaml:"backend_type"`

	// Local disk backend settings
	DataDir string `yaml:"data_dir"`

	// LocalStack settings (only used if backend_type is "localstack")
	LocalStackEndpoint string `yaml:"localstack_endpoint"`

	// AWS settings (for upstream source)
	AWSRegion string `yaml:"aws_region"`

	// Bucket mappings: local bucket name -> AWS bucket name
	BucketMappings map[string]string `yaml:"bucket_mappings"`

	// Buckets to create on startup
	InitBuckets []string `yaml:"init_buckets"`
}

// DefaultConfig returns configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:         ":9000",
		BackendType:        "disk",
		DataDir:            "/data",
		LocalStackEndpoint: "http://localhost:4566",
		AWSRegion:          "us-east-1",
		BucketMappings:     make(map[string]string),
		InitBuckets:        []string{},
	}
}

// LoadConfig loads configuration from file and environment variables.
// Priority: Environment variables override config file values which override defaults.
func LoadConfig() *Config {
	cfg := DefaultConfig()

	// Load from config file if specified
	if configFile := os.Getenv("S3LAZY_CONFIG_FILE"); configFile != "" {
		data, err := os.ReadFile(configFile)
		if err != nil {
			log.Printf("Warning: failed to read config file %s: %v", configFile, err)
		} else if err := yaml.Unmarshal(data, cfg); err != nil {
			log.Printf("Warning: failed to parse config file %s: %v", configFile, err)
		}
	}

	// Environment variables override config file
	if v := os.Getenv("S3LAZY_LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("S3LAZY_BACKEND"); v != "" {
		cfg.BackendType = v
	}
	if v := os.Getenv("S3LAZY_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("S3LAZY_LOCALSTACK_ENDPOINT"); v != "" {
		cfg.LocalStackEndpoint = v
	}
	if v := os.Getenv("S3LAZY_AWS_REGION"); v != "" {
		cfg.AWSRegion = v
	}
	// Also support standard AWS_REGION
	if v := os.Getenv("AWS_REGION"); v != "" && os.Getenv("S3LAZY_AWS_REGION") == "" {
		cfg.AWSRegion = v
	}

	// Parse init buckets from comma-separated list
	if v := os.Getenv("S3LAZY_INIT_BUCKETS"); v != "" {
		cfg.InitBuckets = parseCommaSeparated(v)
	}

	// Parse bucket mappings from "local1:aws1,local2:aws2" format
	if v := os.Getenv("S3LAZY_BUCKET_MAP"); v != "" {
		for _, mapping := range parseCommaSeparated(v) {
			parts := strings.SplitN(mapping, ":", 2)
			if len(parts) == 2 {
				cfg.BucketMappings[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	return cfg
}

// parseCommaSeparated splits a comma-separated string and trims whitespace
func parseCommaSeparated(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
