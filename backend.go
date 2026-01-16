package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
)

// LazyBackend wraps any gofakes3.Backend and adds lazy-loading from AWS S3.
// When an object is not found locally, it fetches from AWS and caches it.
type LazyBackend struct {
	local     gofakes3.Backend
	awsClient *s3.Client

	mu            sync.RWMutex
	bucketMapping map[string]string
}

// NewLazyBackend creates a new lazy-loading backend wrapper.
func NewLazyBackend(local gofakes3.Backend, awsClient *s3.Client) *LazyBackend {
	return &LazyBackend{
		local:         local,
		awsClient:     awsClient,
		bucketMapping: make(map[string]string),
	}
}

// SetBucketMappings sets all bucket mappings at once.
func (b *LazyBackend) SetBucketMappings(mappings map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bucketMapping = make(map[string]string)
	for k, v := range mappings {
		b.bucketMapping[k] = v
	}
}

func (b *LazyBackend) awsBucketName(localBucket string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if mapped, ok := b.bucketMapping[localBucket]; ok {
		return mapped
	}
	return localBucket
}

// isNotFound checks if an error indicates the object was not found
func isNotFound(err error) bool {
	return gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) ||
		gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket)
}

// GetObject tries local cache first, then fetches from AWS and caches locally.
func (b *LazyBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	// Try local cache first
	obj, err := b.local.GetObject(bucketName, objectName, rangeRequest)
	if err == nil {
		log.Printf("[CACHE HIT] %s/%s", bucketName, objectName)
		return obj, nil
	}

	// Check if it's a "not found" error vs other errors
	if !isNotFound(err) {
		return nil, err
	}

	log.Printf("[CACHE MISS] %s/%s - fetching from AWS", bucketName, objectName)

	// Fetch from AWS
	awsBucket := b.awsBucketName(bucketName)
	awsObj, err := b.awsClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(awsBucket),
		Key:    aws.String(objectName),
	})
	if err != nil {
		log.Printf("[AWS ERROR] %s/%s: %v", awsBucket, objectName, err)
		return nil, gofakes3.KeyNotFound(objectName)
	}
	defer awsObj.Body.Close()

	// Get size from AWS response
	var size int64
	if awsObj.ContentLength != nil {
		size = *awsObj.ContentLength
	}

	// Extract metadata
	meta := make(map[string]string)
	if awsObj.ContentType != nil {
		meta["Content-Type"] = *awsObj.ContentType
	}
	for k, v := range awsObj.Metadata {
		meta[k] = v
	}

	// Stream directly to local cache (no memory buffering)
	log.Printf("[CACHING] %s/%s (%d bytes)", bucketName, objectName, size)
	_, err = b.local.PutObject(bucketName, objectName, meta, awsObj.Body, size, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to cache %s/%s: %w", bucketName, objectName, err)
	}

	// Return from local cache
	return b.local.GetObject(bucketName, objectName, rangeRequest)
}

// HeadObject checks local first, then AWS. Does not cache on HEAD.
func (b *LazyBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	obj, err := b.local.HeadObject(bucketName, objectName)
	if err == nil {
		return obj, nil
	}

	if !isNotFound(err) {
		return nil, err
	}

	// Check AWS (but don't cache on HEAD - wait for actual GET)
	awsBucket := b.awsBucketName(bucketName)
	awsObj, err := b.awsClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(awsBucket),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	// Return a minimal Object for HEAD response
	meta := make(map[string]string)
	if awsObj.ContentType != nil {
		meta["Content-Type"] = *awsObj.ContentType
	}

	var size int64
	if awsObj.ContentLength != nil {
		size = *awsObj.ContentLength
	}

	return &gofakes3.Object{
		Name:     objectName,
		Metadata: meta,
		Size:     size,
		Contents: io.NopCloser(&emptyReader{}),
	}, nil
}

// CopyObject ensures source exists locally (triggering lazy fetch if needed), then copies.
func (b *LazyBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	// Ensure source exists locally (this will fetch from AWS if needed)
	_, err := b.GetObject(srcBucket, srcKey, nil)
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}

	// Now do the copy locally
	return b.local.CopyObject(srcBucket, srcKey, dstBucket, dstKey, meta)
}

// Delegate all other methods to local backend

func (b *LazyBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	return b.local.ListBuckets()
}

func (b *LazyBackend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	return b.local.ListBucket(name, prefix, page)
}

func (b *LazyBackend) BucketExists(name string) (bool, error) {
	return b.local.BucketExists(name)
}

func (b *LazyBackend) CreateBucket(name string) error {
	return b.local.CreateBucket(name)
}

func (b *LazyBackend) DeleteBucket(name string) error {
	return b.local.DeleteBucket(name)
}

func (b *LazyBackend) ForceDeleteBucket(name string) error {
	return b.local.ForceDeleteBucket(name)
}

func (b *LazyBackend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	return b.local.PutObject(bucketName, objectName, meta, input, size, conditions)
}

func (b *LazyBackend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	return b.local.DeleteObject(bucketName, objectName)
}

func (b *LazyBackend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	return b.local.DeleteMulti(bucketName, objects...)
}

// emptyReader returns EOF immediately, used for HEAD responses
type emptyReader struct{}

func (r *emptyReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}
