# s3lazy

[![Docker Image](https://img.shields.io/docker/v/rjpr/s3lazy?label=docker)](https://hub.docker.com/r/rjpr/s3lazy)
[![GitHub Container Registry](https://img.shields.io/badge/ghcr.io-rjpr%2Fs3lazy-blue?logo=github)](https://github.com/rjpr/s3lazy/pkgs/container/s3lazy)

Ever accidentally broken a shared dev environment by modifying S3 objects during local testing?

s3lazy is a lazy-loading S3 proxy. Point your app at it instead of AWS, and objects are fetched and stored locally on first access. Your changes stay isolated. Your teammates stay happy.

## Use Cases

- **Local development**: Work with production S3 data without downloading everything upfront
- **CI/CD pipelines**: Cache only the S3 objects your tests actually need
- **Cost optimization**: Reduce S3 GET requests by caching frequently accessed objects
- **Offline development**: Once cached, objects are available without AWS connectivity

## How It Works

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│    Your     │────────▶│   s3lazy    │────────▶│   AWS S3    │
│    App      │◀────────│   (cache)   │◀────────│  (upstream) │
└─────────────┘         └─────────────┘         └─────────────┘
                              │
                              ▼
                        ┌─────────────┐
                        │  Local Disk │
                        │   or        │
                        │  LocalStack │
                        └─────────────┘
```

1. Your app makes an S3 request to s3lazy
2. s3lazy checks local cache (disk or LocalStack)
3. If found: returns cached object (cache hit)
4. If not found: fetches from AWS S3, caches locally, returns object
5. Subsequent requests are served from cache

## Quick Start

### Docker (Recommended)

```bash
docker run -p 9000:9000 \
  -v ./data:/data \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  -e S3LAZY_INIT_BUCKETS=my-bucket \
  rjpr/s3lazy
```

### Docker Compose

```yaml
services:
  s3lazy:
    image: rjpr/s3lazy
    ports:
      - "9000:9000"
    volumes:
      - ./data:/data
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - S3LAZY_INIT_BUCKETS=my-bucket,another-bucket
      - S3LAZY_BUCKET_MAP=my-bucket:prod-bucket-name
```

> **Tip:** Mount your AWS credentials with `-v ~/.aws:/home/s3lazy/.aws:ro` instead of using access keys. Set `AWS_PROFILE` if not using the default profile.

### From Source

```bash
# Build
go build -o s3lazy .

# Run
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export S3LAZY_INIT_BUCKETS=my-bucket
./s3lazy
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3LAZY_LISTEN_ADDR` | `:9000` | HTTP listen address |
| `S3LAZY_BACKEND` | `disk` | Backend type: `disk`, `memory`, or `localstack` |
| `S3LAZY_DATA_DIR` | `/data` | Data directory for disk backend |
| `S3LAZY_LOCALSTACK_ENDPOINT` | `http://localhost:4566` | LocalStack endpoint |
| `S3LAZY_AWS_REGION` | `us-east-1` | AWS region for upstream |
| `S3LAZY_CONFIG_FILE` | | Path to YAML config file |
| `S3LAZY_INIT_BUCKETS` | | Comma-separated bucket names to create on startup |
| `S3LAZY_BUCKET_MAP` | | Bucket mappings as `local1:aws1,local2:aws2` |

Standard AWS environment variables are also supported:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_PROFILE`

### Config File

For complex configurations, use a YAML file:

```yaml
listen_addr: ":9000"
backend_type: "disk"
data_dir: "/data"
aws_region: "us-east-1"

init_buckets:
  - "my-dev-bucket"
  - "another-bucket"

bucket_mappings:
  my-dev-bucket: "production-bucket"
  test-data: "prod-test-data"
```

Set `S3LAZY_CONFIG_FILE=/path/to/config.yaml` to use it.

## Backend Types

### Disk (Default)

Stores cached objects on local disk. Objects persist across restarts.

```bash
S3LAZY_BACKEND=disk
S3LAZY_DATA_DIR=/data
```

### Memory

In-memory storage. Fast but ephemeral—data is lost when the process stops. Useful for CI/CD pipelines or testing.

```bash
S3LAZY_BACKEND=memory
```

### LocalStack

Use an external LocalStack instance as the cache layer. Useful if you're already running LocalStack.

```bash
S3LAZY_BACKEND=localstack
S3LAZY_LOCALSTACK_ENDPOINT=http://localhost:4566
```

## Bucket Mappings

Map local bucket names to different AWS bucket names. This is useful when your development environment uses different bucket names than production.

```bash
# Environment variable format
S3LAZY_BUCKET_MAP=dev-bucket:prod-bucket,test-data:prod-test-data

# YAML format
bucket_mappings:
  dev-bucket: prod-bucket
  test-data: prod-test-data
```

With this configuration:
- Requests to `dev-bucket` are fetched from AWS bucket `prod-bucket`
- Requests to `test-data` are fetched from AWS bucket `prod-test-data`

## Using with AWS SDKs

### Python (boto3)

```python
import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

# First access fetches from AWS and caches
s3.download_file('my-bucket', 'path/to/file.txt', 'local-file.txt')

# Second access serves from cache
s3.download_file('my-bucket', 'path/to/file.txt', 'local-file.txt')
```

### JavaScript (AWS SDK v3)

```javascript
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({
  endpoint: "http://localhost:9000",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  forcePathStyle: true
});

await s3.send(new GetObjectCommand({
  Bucket: "my-bucket",
  Key: "path/to/file.txt"
}));
```

### AWS CLI

```bash
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt .
```

## Health Check

s3lazy exposes a health endpoint at `/health`:

```bash
curl http://localhost:9000/health
# Returns: OK
```

## Logs

s3lazy logs cache hits and misses:

```
[CACHE HIT] my-bucket/path/to/file.txt
[CACHE MISS] my-bucket/path/to/new-file.txt - fetching from AWS
[CACHING] my-bucket/path/to/new-file.txt (1024 bytes)
```

## Development

### Running Tests

```bash
# Run unit tests (fast, no Docker required)
make test-unit

# Run all tests including integration (requires Docker)
make test-all

# Run tests with coverage report
make coverage
```

### Test Structure

- **Unit tests** (`*_test.go`): Test core logic with in-memory mocks
- **Integration tests** (`localstack_test.go`): Test against real LocalStack via testcontainers
  - Requires Docker
  - Automatically spins up/tears down LocalStack containers
  - Skips gracefully if Docker unavailable

### Coverage

```bash
make coverage       # Show coverage by function
make coverage-html  # Generate HTML report
```

## Acknowledgements

Built on [gofakes3](https://github.com/johannesboyne/gofakes3), a fake S3 server implementation in Go.

## License

MIT
