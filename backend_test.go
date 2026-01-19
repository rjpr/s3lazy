package main

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// setupTestBackends creates a LazyBackend with:
// - local: in-memory s3mem backend (simulates LocalStack/disk cache)
// - aws: another s3mem backend behind a test HTTP server (simulates real AWS)
func setupTestBackends(t *testing.T) (*LazyBackend, gofakes3.Backend, gofakes3.Backend, *httptest.Server) {
	t.Helper()

	// Create "local" backend (cache)
	localBackend := s3mem.New()

	// Create "AWS" backend (upstream) with its own gofakes3 server
	awsBackend := s3mem.New()
	awsFaker := gofakes3.New(awsBackend)
	awsServer := httptest.NewServer(awsFaker.Server())
	t.Cleanup(func() { awsServer.Close() })

	// Create S3 client pointing to our fake AWS server
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	awsClient := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(awsServer.URL)
		o.UsePathStyle = true
	})

	// Create the LazyBackend
	lazyBackend := NewLazyBackend(localBackend, awsClient)

	return lazyBackend, localBackend, awsBackend, awsServer
}

func TestLazyBackend_CacheHit(t *testing.T) {
	lazyBackend, localBackend, _, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create bucket and object in LOCAL backend (cache)
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	content := []byte("cached content")
	_, err := localBackend.PutObject("test-bucket", "cached-file.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Get object through LazyBackend - should hit cache
	obj, err := lazyBackend.GetObject("test-bucket", "cached-file.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}

	if string(data) != "cached content" {
		t.Errorf("Content = %q, want %q", string(data), "cached content")
	}
}

func TestLazyBackend_CacheMiss_FetchFromAWS(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create bucket in BOTH backends (local needs bucket to exist for caching)
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Put object ONLY in AWS backend
	awsContent := []byte("content from AWS")
	_, err := awsBackend.PutObject("test-bucket", "aws-only-file.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(awsContent), int64(len(awsContent)), nil)
	if err != nil {
		t.Fatalf("Failed to put object in AWS: %v", err)
	}

	// Get object through LazyBackend - should miss cache and fetch from AWS
	obj, err := lazyBackend.GetObject("test-bucket", "aws-only-file.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}

	if string(data) != "content from AWS" {
		t.Errorf("Content = %q, want %q", string(data), "content from AWS")
	}

	// Verify it was cached locally
	cachedObj, err := localBackend.GetObject("test-bucket", "aws-only-file.txt", nil)
	if err != nil {
		t.Fatalf("Object should be cached locally: %v", err)
	}
	defer cachedObj.Contents.Close()

	cachedData, err := io.ReadAll(cachedObj.Contents)
	if err != nil {
		t.Fatalf("Failed to read cached contents: %v", err)
	}
	if string(cachedData) != "content from AWS" {
		t.Errorf("Cached content = %q, want %q", string(cachedData), "content from AWS")
	}
}

func TestLazyBackend_BucketMapping(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Set up bucket mapping: local-bucket -> aws-prod-bucket
	lazyBackend.SetBucketMappings(map[string]string{
		"local-bucket": "aws-prod-bucket",
	})

	// Create local bucket (for caching)
	if err := localBackend.CreateBucket("local-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}

	// Create AWS bucket with DIFFERENT name
	if err := awsBackend.CreateBucket("aws-prod-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Put object in AWS bucket (with prod name)
	awsContent := []byte("production data")
	_, err := awsBackend.PutObject("aws-prod-bucket", "data.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(awsContent), int64(len(awsContent)), nil)
	if err != nil {
		t.Fatalf("Failed to put object in AWS: %v", err)
	}

	// Request using LOCAL bucket name - should map to AWS bucket
	obj, err := lazyBackend.GetObject("local-bucket", "data.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}

	if string(data) != "production data" {
		t.Errorf("Content = %q, want %q", string(data), "production data")
	}

	// Verify cached under LOCAL bucket name
	cachedObj, err := localBackend.GetObject("local-bucket", "data.txt", nil)
	if err != nil {
		t.Fatalf("Object should be cached under local bucket name: %v", err)
	}
	cachedObj.Contents.Close()
}

func TestLazyBackend_NotFound_BothBackends(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create buckets but NO objects
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Request non-existent object
	_, err := lazyBackend.GetObject("test-bucket", "nonexistent.txt", nil)
	if err == nil {
		t.Fatal("Expected error for non-existent object")
	}

	// Should return a proper "not found" error
	if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("Expected ErrNoSuchKey, got: %v", err)
	}
}

func TestLazyBackend_HeadObject_LocalCacheHit(t *testing.T) {
	lazyBackend, localBackend, _, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}

	// Put object in LOCAL only (not in AWS)
	localContent := []byte("local cached content")
	_, err := localBackend.PutObject("test-bucket", "local-only.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(localContent), int64(len(localContent)), nil)
	if err != nil {
		t.Fatalf("Failed to put object locally: %v", err)
	}

	// HEAD should succeed from local cache without hitting AWS
	obj, err := lazyBackend.HeadObject("test-bucket", "local-only.txt")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	obj.Contents.Close()

	if obj.Size != int64(len(localContent)) {
		t.Errorf("Size = %d, want %d", obj.Size, len(localContent))
	}
}

func TestLazyBackend_HeadObject_NotFoundBoth(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// HEAD for non-existent object should fail
	_, err := lazyBackend.HeadObject("test-bucket", "nonexistent.txt")
	if err == nil {
		t.Error("HeadObject should fail for non-existent object")
	}
}

func TestLazyBackend_HeadObject_NoCache(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create buckets
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Put object ONLY in AWS
	awsContent := []byte("aws content for head test")
	_, err := awsBackend.PutObject("test-bucket", "head-test.txt",
		map[string]string{"Content-Type": "application/octet-stream"},
		bytes.NewReader(awsContent), int64(len(awsContent)), nil)
	if err != nil {
		t.Fatalf("Failed to put object in AWS: %v", err)
	}

	// HEAD should succeed (checking AWS)
	obj, err := lazyBackend.HeadObject("test-bucket", "head-test.txt")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	obj.Contents.Close()

	if obj.Size != int64(len(awsContent)) {
		t.Errorf("Size = %d, want %d", obj.Size, len(awsContent))
	}

	// Verify it was NOT cached (HEAD doesn't cache)
	_, err = localBackend.GetObject("test-bucket", "head-test.txt", nil)
	if err == nil {
		t.Error("HEAD should not cache the object")
	}
}

func TestLazyBackend_CopyObject_LazyFetchesSource(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create buckets
	if err := localBackend.CreateBucket("src-bucket"); err != nil {
		t.Fatalf("Failed to create src bucket: %v", err)
	}
	if err := localBackend.CreateBucket("dst-bucket"); err != nil {
		t.Fatalf("Failed to create dst bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("src-bucket"); err != nil {
		t.Fatalf("Failed to create AWS src bucket: %v", err)
	}

	// Put source object ONLY in AWS
	srcContent := []byte("source content to copy")
	_, err := awsBackend.PutObject("src-bucket", "source.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(srcContent), int64(len(srcContent)), nil)
	if err != nil {
		t.Fatalf("Failed to put source object: %v", err)
	}

	// Copy should lazy-fetch source from AWS, then copy locally
	_, err = lazyBackend.CopyObject("src-bucket", "source.txt", "dst-bucket", "dest.txt", nil)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination exists locally
	dstObj, err := localBackend.GetObject("dst-bucket", "dest.txt", nil)
	if err != nil {
		t.Fatalf("Destination object should exist: %v", err)
	}
	defer dstObj.Contents.Close()

	dstData, err := io.ReadAll(dstObj.Contents)
	if err != nil {
		t.Fatalf("Failed to read dest contents: %v", err)
	}
	if string(dstData) != "source content to copy" {
		t.Errorf("Dest content = %q, want %q", string(dstData), "source content to copy")
	}

	// Verify source was also cached
	srcObj, err := localBackend.GetObject("src-bucket", "source.txt", nil)
	if err != nil {
		t.Fatalf("Source should be cached after copy: %v", err)
	}
	srcObj.Contents.Close()
}

func TestLazyBackend_PutObject_LocalOnly(t *testing.T) {
	lazyBackend, localBackend, _, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create local bucket
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Put object through LazyBackend
	content := []byte("new content")
	_, err := lazyBackend.PutObject("test-bucket", "new-file.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify it exists locally
	obj, err := localBackend.GetObject("test-bucket", "new-file.txt", nil)
	if err != nil {
		t.Fatalf("Object should exist locally: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}
	if string(data) != "new content" {
		t.Errorf("Content = %q, want %q", string(data), "new content")
	}
}

func TestLazyBackend_DeleteObject(t *testing.T) {
	lazyBackend, localBackend, _, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create bucket and object locally
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	content := []byte("to be deleted")
	_, err := localBackend.PutObject("test-bucket", "delete-me.txt",
		map[string]string{}, bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Delete through LazyBackend
	_, err = lazyBackend.DeleteObject("test-bucket", "delete-me.txt")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify it's gone
	_, err = localBackend.GetObject("test-bucket", "delete-me.txt", nil)
	if err == nil {
		t.Error("Object should be deleted")
	}
}

func TestLazyBackend_ETag_GetObject(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create buckets
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Put object in AWS
	content := []byte("content for etag test")
	_, err := awsBackend.PutObject("test-bucket", "etag-test.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Failed to put object in AWS: %v", err)
	}

	// Get object through LazyBackend
	obj, err := lazyBackend.GetObject("test-bucket", "etag-test.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer obj.Contents.Close()

	// Hash should be populated (non-nil, non-empty)
	if len(obj.Hash) == 0 {
		t.Error("GetObject should return object with non-empty Hash for ETag")
	}
}

func TestLazyBackend_ETag_HeadObject(t *testing.T) {
	lazyBackend, localBackend, awsBackend, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	// Create buckets
	if err := localBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create local bucket: %v", err)
	}
	if err := awsBackend.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create AWS bucket: %v", err)
	}

	// Put object in AWS
	content := []byte("content for head etag test")
	_, err := awsBackend.PutObject("test-bucket", "head-etag-test.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Failed to put object in AWS: %v", err)
	}

	// HEAD object through LazyBackend (object only in AWS, not cached)
	obj, err := lazyBackend.HeadObject("test-bucket", "head-etag-test.txt")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	defer obj.Contents.Close()

	// Hash should be populated (non-nil, non-empty)
	if len(obj.Hash) == 0 {
		t.Error("HeadObject should return object with non-empty Hash for ETag")
	}
}

func TestParseETagToHash(t *testing.T) {
	tests := []struct {
		name     string
		etag     *string
		wantNil  bool
		wantLen  int
	}{
		{
			name:    "nil etag",
			etag:    nil,
			wantNil: true,
		},
		{
			name:    "valid etag with quotes",
			etag:    strPtr("\"d41d8cd98f00b204e9800998ecf8427e\""),
			wantNil: false,
			wantLen: 16, // MD5 is 16 bytes
		},
		{
			name:    "valid etag without quotes",
			etag:    strPtr("d41d8cd98f00b204e9800998ecf8427e"),
			wantNil: false,
			wantLen: 16,
		},
		{
			name:    "invalid hex string",
			etag:    strPtr("not-a-hex-string"),
			wantNil: true,
		},
		{
			name:    "empty string",
			etag:    strPtr(""),
			wantNil: false,
			wantLen: 0, // empty hex decodes to empty slice
		},
		{
			name:    "multipart etag (not valid hex)",
			etag:    strPtr("\"d41d8cd98f00b204e9800998ecf8427e-2\""),
			wantNil: true, // contains dash, not valid hex
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseETagToHash(tt.etag)
			if tt.wantNil {
				if result != nil {
					t.Errorf("parseETagToHash() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Error("parseETagToHash() = nil, want non-nil")
				} else if len(result) != tt.wantLen {
					t.Errorf("parseETagToHash() len = %d, want %d", len(result), tt.wantLen)
				}
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}

// TestLazyBackend_Passthroughs tests that pass-through methods correctly delegate to local backend
func TestLazyBackend_Passthroughs(t *testing.T) {
	lazyBackend, localBackend, _, awsServer := setupTestBackends(t)
	defer awsServer.Close()

	t.Run("CreateBucket", func(t *testing.T) {
		err := lazyBackend.CreateBucket("passthrough-bucket")
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}
		// Verify it exists in local
		exists, err := localBackend.BucketExists("passthrough-bucket")
		if err != nil {
			t.Fatalf("BucketExists failed: %v", err)
		}
		if !exists {
			t.Error("Bucket should exist in local backend after CreateBucket")
		}
	})

	t.Run("BucketExists", func(t *testing.T) {
		exists, err := lazyBackend.BucketExists("passthrough-bucket")
		if err != nil {
			t.Fatalf("BucketExists failed: %v", err)
		}
		if !exists {
			t.Error("BucketExists should return true for existing bucket")
		}

		exists, err = lazyBackend.BucketExists("nonexistent-bucket")
		if err != nil {
			t.Fatalf("BucketExists failed: %v", err)
		}
		if exists {
			t.Error("BucketExists should return false for non-existing bucket")
		}
	})

	t.Run("ListBuckets", func(t *testing.T) {
		buckets, err := lazyBackend.ListBuckets()
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}
		found := false
		for _, b := range buckets {
			if b.Name == "passthrough-bucket" {
				found = true
				break
			}
		}
		if !found {
			t.Error("ListBuckets should include passthrough-bucket")
		}
	})

	t.Run("ListBucket", func(t *testing.T) {
		// Put some objects first
		content := []byte("test content")
		_, err := lazyBackend.PutObject("passthrough-bucket", "file1.txt", nil,
			bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		_, err = lazyBackend.PutObject("passthrough-bucket", "file2.txt", nil,
			bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		list, err := lazyBackend.ListBucket("passthrough-bucket", nil, gofakes3.ListBucketPage{})
		if err != nil {
			t.Fatalf("ListBucket failed: %v", err)
		}
		if len(list.Contents) != 2 {
			t.Errorf("ListBucket should return 2 objects, got %d", len(list.Contents))
		}
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		_, err := lazyBackend.DeleteMulti("passthrough-bucket", "file1.txt", "file2.txt")
		if err != nil {
			t.Fatalf("DeleteMulti failed: %v", err)
		}
		// Verify deleted
		list, err := lazyBackend.ListBucket("passthrough-bucket", nil, gofakes3.ListBucketPage{})
		if err != nil {
			t.Fatalf("ListBucket failed: %v", err)
		}
		if len(list.Contents) != 0 {
			t.Errorf("ListBucket should return 0 objects after DeleteMulti, got %d", len(list.Contents))
		}
	})

	t.Run("ForceDeleteBucket", func(t *testing.T) {
		// Put an object so bucket isn't empty
		content := []byte("content")
		_, err := lazyBackend.PutObject("passthrough-bucket", "leftover.txt", nil,
			bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		err = lazyBackend.ForceDeleteBucket("passthrough-bucket")
		if err != nil {
			t.Fatalf("ForceDeleteBucket failed: %v", err)
		}

		exists, _ := lazyBackend.BucketExists("passthrough-bucket")
		if exists {
			t.Error("Bucket should not exist after ForceDeleteBucket")
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		// Create and delete an empty bucket
		err := lazyBackend.CreateBucket("delete-me-bucket")
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}
		err = lazyBackend.DeleteBucket("delete-me-bucket")
		if err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}
		exists, _ := lazyBackend.BucketExists("delete-me-bucket")
		if exists {
			t.Error("Bucket should not exist after DeleteBucket")
		}
	})
}
