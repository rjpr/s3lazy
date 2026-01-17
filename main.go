package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/spf13/afero"
)

func main() {
	// Load configuration
	cfg := LoadConfig()

	log.Printf("s3lazy starting with backend=%s", cfg.BackendType)

	// Create AWS client for upstream (real AWS)
	awsClient, err := createAWSClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create AWS client: %v", err)
	}

	// Create local backend based on configuration
	localBackend, err := createLocalBackend(cfg)
	if err != nil {
		log.Fatalf("Failed to create local backend: %v", err)
	}

	// Wrap with lazy-loading
	lazyBackend := NewLazyBackend(localBackend, awsClient)

	// Set bucket mappings
	if len(cfg.BucketMappings) > 0 {
		lazyBackend.SetBucketMappings(cfg.BucketMappings)
		log.Printf("Configured %d bucket mapping(s)", len(cfg.BucketMappings))
	}

	// Initialize buckets
	for _, bucket := range cfg.InitBuckets {
		if err := lazyBackend.CreateBucket(bucket); err != nil {
			log.Printf("Warning: couldn't create bucket %s: %v", bucket, err)
		} else {
			log.Printf("Created bucket: %s", bucket)
		}
	}

	// Create gofakes3 server
	faker := gofakes3.New(lazyBackend,
		gofakes3.WithLogger(gofakes3.StdLog(log.Default())),
	)

	// Create HTTP server with health check
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.Handle("/", faker.Server())

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	// Graceful shutdown handling
	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Fatalf("Server forced to shutdown: %v", err)
		}

		close(done)
	}()

	// Start server
	log.Printf("Starting lazy-loading S3 proxy on %s", cfg.ListenAddr)
	log.Printf("Backend type: %s", cfg.BackendType)
	if cfg.BackendType == "local" {
		log.Printf("Data directory: %s", cfg.DataDir)
	} else {
		log.Printf("LocalStack endpoint: %s", cfg.LocalStackEndpoint)
	}
	log.Printf("Health check: http://localhost%s/health", cfg.ListenAddr)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}

	<-done
	log.Println("Server stopped")
}

// createAWSClient creates an S3 client for the real AWS endpoint
func createAWSClient(cfg *Config) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

// createLocalBackend creates the local storage backend based on configuration
func createLocalBackend(cfg *Config) (gofakes3.Backend, error) {
	switch cfg.BackendType {
	case "localstack":
		log.Printf("Using LocalStack backend at %s", cfg.LocalStackEndpoint)
		return NewLocalStackBackend(cfg.LocalStackEndpoint, cfg.AWSRegion)

	case "local":
		log.Printf("Using disk-based backend at %s", cfg.DataDir)

		// Ensure data directory exists
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return nil, err
		}

		// Create filesystem-based backend using afero
		fs := afero.NewBasePathFs(afero.NewOsFs(), cfg.DataDir)
		return s3afero.MultiBucket(fs)

	default:
		return nil, fmt.Errorf("unknown backend type: %q (valid options: local, localstack)", cfg.BackendType)
	}
}

// healthHandler returns OK if the server is running
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
