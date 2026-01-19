//go:build integration

package main

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

// skipIfNoDocker skips the test if Docker is not available
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not found, skipping integration test")
	}
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		t.Skip("Docker daemon not running, skipping integration test")
	}
}

// localstackTestContainer holds the container and endpoint for tests
type localstackTestContainer struct {
	container *localstack.LocalStackContainer
	endpoint  string
	ctx       context.Context
}

// setupLocalStack creates a LocalStack container and returns the test container
func setupLocalStack(t *testing.T) *localstackTestContainer {
	t.Helper()
	skipIfNoDocker(t)

	ctx := context.Background()

	container, err := localstack.Run(ctx,
		"localstack/localstack:4.12",
		testcontainers.WithEnv(map[string]string{
			"SERVICES":           "s3",
			"AWS_DEFAULT_REGION": "us-east-1",
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start LocalStack container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		testcontainers.TerminateContainer(container)
		t.Fatalf("Failed to get container host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		testcontainers.TerminateContainer(container)
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	endpoint := "http://" + host + ":" + mappedPort.Port()

	return &localstackTestContainer{
		container: container,
		endpoint:  endpoint,
		ctx:       ctx,
	}
}

// teardown terminates the container
func (tc *localstackTestContainer) teardown(t *testing.T) {
	t.Helper()
	if err := testcontainers.TerminateContainer(tc.container); err != nil {
		t.Logf("Warning: failed to terminate container: %v", err)
	}
}

// newBackend creates a LocalStackBackend connected to the test container
func (tc *localstackTestContainer) newBackend(t *testing.T, region string) *LocalStackBackend {
	t.Helper()
	backend, err := NewLocalStackBackend(tc.endpoint, region)
	if err != nil {
		t.Fatalf("Failed to create LocalStackBackend: %v", err)
	}
	return backend
}

func TestLocalStackBackend_CreateBucket_Regions(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	// Test that CreateBucket works correctly across regions:
	// - us-east-1: must NOT send LocationConstraint (AWS default region quirk)
	// - other regions: must send LocationConstraint
	tests := []struct {
		name   string
		region string
		bucket string
	}{
		{"us-east-1 (no LocationConstraint)", "us-east-1", "test-bucket-useast1"},
		{"eu-west-1", "eu-west-1", "test-bucket-euwest1"},
		{"ap-southeast-2", "ap-southeast-2", "test-bucket-apsoutheast2"},
		{"us-west-2", "us-west-2", "test-bucket-uswest2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := tc.newBackend(t, tt.region)

			err := backend.CreateBucket(tt.bucket)
			if err != nil {
				t.Fatalf("CreateBucket failed for region %s: %v", tt.region, err)
			}

			// Verify bucket exists
			exists, err := backend.BucketExists(tt.bucket)
			if err != nil {
				t.Fatalf("BucketExists failed: %v", err)
			}
			if !exists {
				t.Errorf("Bucket %s should exist after creation", tt.bucket)
			}

			// Cleanup
			backend.DeleteBucket(tt.bucket)
		})
	}
}

func TestLocalStackBackend_PutGetObject(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-crud-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// Put object
	content := []byte("hello localstack")
	_, err := backend.PutObject(bucket, "test-key.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Get object
	obj, err := backend.GetObject(bucket, "test-key.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}

	if string(data) != "hello localstack" {
		t.Errorf("Content = %q, want %q", string(data), "hello localstack")
	}
}

func TestLocalStackBackend_HeadObject(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-head-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	content := []byte("content for head test")
	_, err := backend.PutObject(bucket, "head-test.txt",
		map[string]string{"Content-Type": "text/plain"},
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	obj, err := backend.HeadObject(bucket, "head-test.txt")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	obj.Contents.Close()

	if obj.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", obj.Size, len(content))
	}
}

func TestLocalStackBackend_NotFound(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-notfound-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// GetObject non-existent key -> ErrNoSuchKey
	_, err := backend.GetObject(bucket, "nonexistent.txt", nil)
	if err == nil {
		t.Error("Expected error for non-existent object")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("GetObject non-existent key: expected ErrNoSuchKey, got: %v", err)
	}

	// HeadObject non-existent key -> "NotFound" (S3 HEAD requests return generic 404)
	_, err = backend.HeadObject(bucket, "nonexistent.txt")
	if err == nil {
		t.Error("Expected error for HeadObject on non-existent object")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrorCode("NotFound")) {
		t.Errorf("HeadObject non-existent key: expected NotFound, got: %v", err)
	}

	// HeadObject non-existent bucket -> "NotFound" (S3 HEAD requests return generic 404,
	// cannot distinguish between missing bucket and missing key - this matches real S3 behavior)
	_, err = backend.HeadObject("nonexistent-bucket-xyz", "file.txt")
	if err == nil {
		t.Error("Expected error for HeadObject on non-existent bucket")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrorCode("NotFound")) {
		t.Errorf("HeadObject non-existent bucket: expected NotFound, got: %v", err)
	}

	// GetObject non-existent bucket -> ErrNoSuchBucket
	_, err = backend.GetObject("nonexistent-bucket-xyz", "file.txt", nil)
	if err == nil {
		t.Error("Expected error for GetObject on non-existent bucket")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket) {
		t.Errorf("GetObject non-existent bucket: expected ErrNoSuchBucket, got: %v", err)
	}

	// ListBucket non-existent bucket -> ErrNoSuchBucket
	_, err = backend.ListBucket("nonexistent-bucket-xyz", nil, gofakes3.ListBucketPage{})
	if err == nil {
		t.Error("Expected error for ListBucket on non-existent bucket")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket) {
		t.Errorf("ListBucket non-existent bucket: expected ErrNoSuchBucket, got: %v", err)
	}

	// CopyObject non-existent source key -> ErrNoSuchKey
	_, err = backend.CopyObject(bucket, "nonexistent.txt", bucket, "dest.txt", nil)
	if err == nil {
		t.Error("Expected error for CopyObject with non-existent source key")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("CopyObject non-existent source key: expected ErrNoSuchKey, got: %v", err)
	}

	// CopyObject non-existent source bucket -> ErrNoSuchBucket
	_, err = backend.CopyObject("nonexistent-bucket-xyz", "file.txt", bucket, "dest.txt", nil)
	if err == nil {
		t.Error("Expected error for CopyObject with non-existent source bucket")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket) {
		t.Errorf("CopyObject non-existent source bucket: expected ErrNoSuchBucket, got: %v", err)
	}

	// DeleteBucket non-existent -> ErrNoSuchBucket
	err = backend.DeleteBucket("nonexistent-bucket-xyz")
	if err == nil {
		t.Error("Expected error for DeleteBucket on non-existent bucket")
	} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket) {
		t.Errorf("DeleteBucket non-existent: expected ErrNoSuchBucket, got: %v", err)
	}
}

func TestLocalStackBackend_ListBucket(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-list-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// Put multiple objects
	for _, key := range []string{"prefix/a.txt", "prefix/b.txt", "other.txt"} {
		content := []byte("content")
		_, err := backend.PutObject(bucket, key, nil,
			bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", key, err)
		}
	}

	// List with prefix
	prefix := &gofakes3.Prefix{HasPrefix: true, Prefix: "prefix/"}
	list, err := backend.ListBucket(bucket, prefix, gofakes3.ListBucketPage{})
	if err != nil {
		t.Fatalf("ListBucket failed: %v", err)
	}

	if len(list.Contents) != 2 {
		t.Errorf("Expected 2 objects with prefix, got %d", len(list.Contents))
	}
}

func TestLocalStackBackend_DeleteObject(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-delete-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	content := []byte("to be deleted")
	_, err := backend.PutObject(bucket, "delete-me.txt", nil,
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Delete
	_, err = backend.DeleteObject(bucket, "delete-me.txt")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify deleted
	_, err = backend.GetObject(bucket, "delete-me.txt", nil)
	if err == nil {
		t.Error("Object should be deleted")
	}
}

func TestLocalStackBackend_CopyObject(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	srcBucket := "test-copy-src"
	dstBucket := "test-copy-dst"

	if err := backend.CreateBucket(srcBucket); err != nil {
		t.Fatalf("CreateBucket (src) failed: %v", err)
	}
	defer backend.ForceDeleteBucket(srcBucket)

	if err := backend.CreateBucket(dstBucket); err != nil {
		t.Fatalf("CreateBucket (dst) failed: %v", err)
	}
	defer backend.ForceDeleteBucket(dstBucket)

	// Put source object
	content := []byte("content to copy")
	_, err := backend.PutObject(srcBucket, "source.txt", nil,
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy to destination
	_, err = backend.CopyObject(srcBucket, "source.txt", dstBucket, "dest.txt", nil)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination exists with correct content
	obj, err := backend.GetObject(dstBucket, "dest.txt", nil)
	if err != nil {
		t.Fatalf("GetObject (dest) failed: %v", err)
	}
	defer obj.Contents.Close()

	data, err := io.ReadAll(obj.Contents)
	if err != nil {
		t.Fatalf("Failed to read contents: %v", err)
	}

	if string(data) != "content to copy" {
		t.Errorf("Content = %q, want %q", string(data), "content to copy")
	}

	// Verify source still exists
	_, err = backend.GetObject(srcBucket, "source.txt", nil)
	if err != nil {
		t.Errorf("Source object should still exist after copy: %v", err)
	}
}

func TestLocalStackBackend_DeleteObject_NonExistent(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-delete-nonexistent"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// Delete a file that doesn't exist - S3 should succeed silently (idempotent)
	_, err := backend.DeleteObject(bucket, "never-existed.txt")
	if err != nil {
		t.Errorf("DeleteObject on non-existent file should succeed, got: %v", err)
	}
}

func TestLocalStackBackend_BucketExists(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")

	// Test non-existent bucket
	exists, err := backend.BucketExists("nonexistent-bucket-12345")
	if err != nil {
		t.Fatalf("BucketExists failed: %v", err)
	}
	if exists {
		t.Error("BucketExists should return false for non-existent bucket")
	}

	// Create bucket and verify it exists
	bucket := "test-exists-bucket"
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.DeleteBucket(bucket)

	exists, err = backend.BucketExists(bucket)
	if err != nil {
		t.Fatalf("BucketExists failed: %v", err)
	}
	if !exists {
		t.Error("BucketExists should return true for existing bucket")
	}
}

func TestLocalStackBackend_ListBuckets(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")

	// Create a few buckets
	buckets := []string{"list-test-alpha", "list-test-beta", "list-test-gamma"}
	for _, name := range buckets {
		if err := backend.CreateBucket(name); err != nil {
			t.Fatalf("CreateBucket %s failed: %v", name, err)
		}
		defer backend.DeleteBucket(name)
	}

	// List all buckets
	result, err := backend.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}

	// Verify our buckets are in the list
	found := make(map[string]bool)
	for _, b := range result {
		found[b.Name] = true
	}

	for _, name := range buckets {
		if !found[name] {
			t.Errorf("Bucket %s not found in ListBuckets result", name)
		}
	}
}

func TestLocalStackBackend_GetObject_RangeRequest(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-range-bucket"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// Put a file with known content
	content := []byte("0123456789abcdefghij") // 20 bytes
	_, err := backend.PutObject(bucket, "range-test.txt", nil,
		bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Run("byte range (start-end)", func(t *testing.T) {
		// Request bytes 5-9 (should return "56789")
		rangeReq := &gofakes3.ObjectRangeRequest{
			Start:   5,
			End:     9,
			FromEnd: false,
		}

		obj, err := backend.GetObject(bucket, "range-test.txt", rangeReq)
		if err != nil {
			t.Fatalf("GetObject with range failed: %v", err)
		}
		defer obj.Contents.Close()

		data, err := io.ReadAll(obj.Contents)
		if err != nil {
			t.Fatalf("Failed to read contents: %v", err)
		}

		if string(data) != "56789" {
			t.Errorf("Range content = %q, want %q", string(data), "56789")
		}
	})

	t.Run("from end (last N bytes)", func(t *testing.T) {
		// Request last 5 bytes (should return "fghij")
		rangeReq := &gofakes3.ObjectRangeRequest{
			End:     5,
			FromEnd: true,
		}

		obj, err := backend.GetObject(bucket, "range-test.txt", rangeReq)
		if err != nil {
			t.Fatalf("GetObject with range (from end) failed: %v", err)
		}
		defer obj.Contents.Close()

		data, err := io.ReadAll(obj.Contents)
		if err != nil {
			t.Fatalf("Failed to read contents: %v", err)
		}

		if string(data) != "fghij" {
			t.Errorf("Range content = %q, want %q", string(data), "fghij")
		}
	})
}

func TestLocalStackBackend_DeleteMulti(t *testing.T) {
	tc := setupLocalStack(t)
	defer tc.teardown(t)

	backend := tc.newBackend(t, "eu-west-1")
	bucket := "test-delete-multi"

	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer backend.ForceDeleteBucket(bucket)

	// Create multiple objects
	keys := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, key := range keys {
		content := []byte("content for " + key)
		_, err := backend.PutObject(bucket, key, nil,
			bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("PutObject %s failed: %v", key, err)
		}
	}

	// Delete multiple objects at once
	_, err := backend.DeleteMulti(bucket, keys...)
	if err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}

	// Verify all objects are deleted
	for _, key := range keys {
		_, err := backend.GetObject(bucket, key, nil)
		if err == nil {
			t.Errorf("Object %s should be deleted", key)
		}
	}
}
