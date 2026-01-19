package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.String() != "OK" {
		t.Errorf("body = %q, want %q", w.Body.String(), "OK")
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain" {
		t.Errorf("Content-Type = %q, want %q", ct, "text/plain")
	}
}

func TestCreateLocalBackend_Disk(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		BackendType: "disk",
		DataDir:     tmpDir,
	}

	backend, err := createLocalBackend(cfg)
	if err != nil {
		t.Fatalf("createLocalBackend failed: %v", err)
	}
	if backend == nil {
		t.Error("backend should not be nil")
	}
}

func TestCreateLocalBackend_DiskCreatesDir(t *testing.T) {
	tmpDir := t.TempDir()
	newDir := filepath.Join(tmpDir, "subdir", "data")

	cfg := &Config{
		BackendType: "disk",
		DataDir:     newDir,
	}

	_, err := createLocalBackend(cfg)
	if err != nil {
		t.Fatalf("createLocalBackend failed: %v", err)
	}

	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		t.Error("data directory should have been created")
	}
}

func TestCreateLocalBackend_Memory(t *testing.T) {
	cfg := &Config{
		BackendType: "memory",
	}

	backend, err := createLocalBackend(cfg)
	if err != nil {
		t.Fatalf("createLocalBackend failed: %v", err)
	}
	if backend == nil {
		t.Error("backend should not be nil")
	}
}

func TestCreateLocalBackend_LocalStack(t *testing.T) {
	cfg := &Config{
		BackendType:        "localstack",
		LocalStackEndpoint: "http://localhost:4566",
		AWSRegion:          "us-east-1",
	}

	backend, err := createLocalBackend(cfg)
	if err != nil {
		t.Fatalf("createLocalBackend failed: %v", err)
	}
	if backend == nil {
		t.Error("backend should not be nil")
	}
}

func TestCreateLocalBackend_DiskInvalidDir(t *testing.T) {
	// Try to create a directory inside a file (which should fail)
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "afile")
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	cfg := &Config{
		BackendType: "disk",
		DataDir:     filepath.Join(filePath, "subdir"), // Can't create dir inside a file
	}

	_, err := createLocalBackend(cfg)
	if err == nil {
		t.Error("expected error when data dir is inside a file")
	}
}

func TestCreateLocalBackend_InvalidType(t *testing.T) {
	cfg := &Config{
		BackendType: "aws",
	}

	_, err := createLocalBackend(cfg)
	if err == nil {
		t.Error("expected error for invalid backend type")
	}
}

func TestCreateAWSClient(t *testing.T) {
	cfg := &Config{
		AWSRegion: "us-east-1",
	}

	client, err := createAWSClient(cfg)
	if err != nil {
		t.Fatalf("createAWSClient failed: %v", err)
	}
	if client == nil {
		t.Error("client should not be nil")
	}
}
