package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/johannesboyne/gofakes3"
)

// LocalStackBackend implements gofakes3.Backend by proxying to an S3-compatible
// service like LocalStack. This allows using LocalStack as the local cache layer.
type LocalStackBackend struct {
	client *s3.Client
	region string
}

// NewLocalStackBackend creates a backend that talks to LocalStack or any S3-compatible service.
func NewLocalStackBackend(endpoint, region string) (*LocalStackBackend, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &LocalStackBackend{client: client, region: region}, nil
}

func (b *LocalStackBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	ctx := context.Background()

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// Handle range requests
	if rangeRequest != nil {
		var rangeStr string
		if rangeRequest.FromEnd {
			rangeStr = fmt.Sprintf("bytes=-%d", rangeRequest.End)
		} else {
			rangeStr = fmt.Sprintf("bytes=%d-%d", rangeRequest.Start, rangeRequest.End)
		}
		input.Range = aws.String(rangeStr)
	}

	obj, err := b.client.GetObject(ctx, input)
	if err != nil {
		return nil, s3ErrorToGofakes3(err, bucketName, objectName)
	}

	return getOutputToObject(objectName, obj), nil
}

func (b *LocalStackBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	ctx := context.Background()

	obj, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return nil, s3ErrorToGofakes3(err, bucketName, objectName)
	}

	return headOutputToObject(objectName, obj), nil
}

func (b *LocalStackBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	ctx := context.Background()

	_, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(srcBucket + "/" + srcKey),
	})
	if err != nil {
		return gofakes3.CopyObjectResult{}, s3ErrorToGofakes3(err, "", "")
	}

	return gofakes3.CopyObjectResult{}, nil
}

func (b *LocalStackBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	ctx := context.Background()

	result, err := b.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, s3ErrorToGofakes3(err, "", "")
	}

	buckets := make([]gofakes3.BucketInfo, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		if bucket.Name == nil || bucket.CreationDate == nil {
			continue
		}
		buckets = append(buckets, gofakes3.BucketInfo{
			Name:         *bucket.Name,
			CreationDate: gofakes3.NewContentTime(*bucket.CreationDate),
		})
	}
	return buckets, nil
}

func (b *LocalStackBackend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	ctx := context.Background()

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(name),
	}
	if prefix != nil && prefix.HasPrefix {
		input.Prefix = aws.String(prefix.Prefix)
	}
	if prefix != nil && prefix.HasDelimiter {
		input.Delimiter = aws.String(prefix.Delimiter)
	}
	if page.HasMarker {
		input.StartAfter = aws.String(page.Marker)
	}
	if page.MaxKeys > 0 {
		input.MaxKeys = aws.Int32(int32(page.MaxKeys))
	}

	result, err := b.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, s3ErrorToGofakes3(err, name, "")
	}

	var objects []*gofakes3.Content
	for _, obj := range result.Contents {
		if obj.Key == nil {
			continue
		}
		content := &gofakes3.Content{
			Key: *obj.Key,
		}
		if obj.Size != nil {
			content.Size = *obj.Size
		}
		if obj.LastModified != nil {
			content.LastModified = gofakes3.NewContentTime(*obj.LastModified)
		}
		if obj.ETag != nil {
			content.ETag = *obj.ETag
		}
		objects = append(objects, content)
	}

	var prefixes []gofakes3.CommonPrefix
	for _, p := range result.CommonPrefixes {
		if p.Prefix != nil {
			prefixes = append(prefixes, gofakes3.CommonPrefix{Prefix: *p.Prefix})
		}
	}

	var isTruncated bool
	if result.IsTruncated != nil {
		isTruncated = *result.IsTruncated
	}

	return &gofakes3.ObjectList{
		Contents:       objects,
		CommonPrefixes: prefixes,
		IsTruncated:    isTruncated,
	}, nil
}

func (b *LocalStackBackend) BucketExists(name string) (bool, error) {
	ctx := context.Background()

	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		code := s3ErrorCode(err)
		// HeadBucket can return NotFound (HTTP 404) or NoSuchBucket
		if code == "NoSuchBucket" || code == "NotFound" {
			return false, nil
		}
		return false, s3ErrorToGofakes3(err, name, "")
	}
	return true, nil
}

func (b *LocalStackBackend) CreateBucket(name string) error {
	ctx := context.Background()

	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}

	// For any region other than us-east-1, we must specify the LocationConstraint.
	// This is an AWS quirk: us-east-1 is the "default" region and must NOT have
	// a LocationConstraint specified, while all other regions require it.
	if b.region != "" && b.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(b.region),
		}
	}

	_, err := b.client.CreateBucket(ctx, input)
	return s3ErrorToGofakes3(err, name, "")
}

func (b *LocalStackBackend) DeleteBucket(name string) error {
	ctx := context.Background()

	_, err := b.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return s3ErrorToGofakes3(err, name, "")
}

func (b *LocalStackBackend) ForceDeleteBucket(name string) error {
	ctx := context.Background()

	// First, delete all objects in the bucket
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(name),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return s3ErrorToGofakes3(err, name, "")
		}

		if len(page.Contents) > 0 {
			var objectIds []s3types.ObjectIdentifier
			for _, obj := range page.Contents {
				objectIds = append(objectIds, s3types.ObjectIdentifier{
					Key: obj.Key,
				})
			}

			_, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(name),
				Delete: &s3types.Delete{
					Objects: objectIds,
				},
			})
			if err != nil {
				return s3ErrorToGofakes3(err, name, "")
			}
		}
	}

	// Now delete the bucket
	_, err := b.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return s3ErrorToGofakes3(err, name, "")
}

func (b *LocalStackBackend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	ctx := context.Background()

	// Read all data (S3 client needs the full body)
	data, err := io.ReadAll(input)
	if err != nil {
		return gofakes3.PutObjectResult{}, err
	}

	putInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Body:   bytes.NewReader(data),
	}
	if ct, ok := meta["Content-Type"]; ok {
		putInput.ContentType = aws.String(ct)
	}

	result, err := b.client.PutObject(ctx, putInput)
	if err != nil {
		return gofakes3.PutObjectResult{}, s3ErrorToGofakes3(err, bucketName, objectName)
	}

	var versionID gofakes3.VersionID
	if result.VersionId != nil {
		versionID = gofakes3.VersionID(*result.VersionId)
	}

	return gofakes3.PutObjectResult{
		VersionID: versionID,
	}, nil
}

func (b *LocalStackBackend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	ctx := context.Background()

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	return gofakes3.ObjectDeleteResult{}, s3ErrorToGofakes3(err, bucketName, objectName)
}

func (b *LocalStackBackend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	ctx := context.Background()

	var objectIds []s3types.ObjectIdentifier
	for _, key := range objects {
		objectIds = append(objectIds, s3types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}

	_, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3types.Delete{
			Objects: objectIds,
		},
	})

	return gofakes3.MultiDeleteResult{}, s3ErrorToGofakes3(err, bucketName, "")
}

// s3ErrorCode extracts the S3 error code from an AWS SDK error.
// Returns empty string if the error doesn't have an error code.
func s3ErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// s3ErrorToGofakes3 converts an AWS SDK error to a gofakes3 error.
// NoSuchBucket/NoSuchKey need resource names for proper error messages;
// all other codes pass through directly.
func s3ErrorToGofakes3(err error, bucketName, objectName string) error {
	if err == nil {
		return nil
	}

	code := s3ErrorCode(err)
	if code == "" {
		// Log the original error for debugging before it becomes a generic InternalError
		log.Printf("[NON-S3 ERROR] %s/%s : %v", bucketName, objectName, err)
		return err
	}

	switch code {
	case "NoSuchBucket":
		return gofakes3.BucketNotFound(bucketName)
	case "NoSuchKey":
		return gofakes3.KeyNotFound(objectName)
	default:
		return gofakes3.ErrorCode(code)
	}
}
