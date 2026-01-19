[![GHCR](https://img.shields.io/badge/Also%20on-GitHub%20Container%20Registry-blue?logo=github)](https://ghcr.io/rjpr/s3lazy)

# s3lazy

Ever accidentally broken a shared dev environment by modifying S3 objects during local testing?

s3lazy is a lazy-loading S3 proxy. Point your app at it instead of AWS, and objects are fetched and stored locally on first access. Your changes stay isolated. Your teammates stay happy.

## Why s3lazy?

- **Local development** - Work with production S3 data without downloading everything upfront
- **CI/CD pipelines** - Cache only the S3 objects your tests actually need
- **Cost optimization** - Reduce S3 GET requests by caching frequently accessed objects
- **Offline development** - Once cached, objects are available without AWS connectivity

## Quick Start

```
docker run -p 9000:9000 \
  -v ./data:/data \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  -e S3LAZY_INIT_BUCKETS=my-bucket \
  rjpr/s3lazy
```

Or with Docker Compose:

```
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
      - S3LAZY_INIT_BUCKETS=my-bucket
      - S3LAZY_BUCKET_MAP=my-bucket:prod-bucket-name
```

Then point your AWS SDK at `http://localhost:9000` instead of AWS.

**Tip:** Mount your AWS credentials with `-v ~/.aws:/home/s3lazy/.aws:ro` instead of using access keys.

## Configuration

- `S3LAZY_BACKEND` - `disk` (default), `memory`, or `localstack`
- `S3LAZY_DATA_DIR` - Cache directory (default: `/data`)
- `S3LAZY_INIT_BUCKETS` - Buckets to create on startup (comma-separated)
- `S3LAZY_BUCKET_MAP` - Map local to AWS bucket names: `local1:aws1,local2:aws2`

See [full configuration options](https://github.com/rjpr/s3lazy#configuration) for all environment variables, YAML config files, and SDK examples.

## Links

- [GitHub](https://github.com/rjpr/s3lazy)
- [Documentation](https://github.com/rjpr/s3lazy#readme)
