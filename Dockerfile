FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 go build -o s3lazy .

FROM alpine:latest

# Install ca-certificates for HTTPS and create non-root user
RUN apk --no-cache add ca-certificates wget && \
    adduser -D -u 1000 s3lazy && \
    mkdir -p /data && \
    chown s3lazy:s3lazy /data

USER s3lazy
WORKDIR /app
COPY --from=builder /app/s3lazy .

# Default configuration
ENV S3LAZY_LISTEN_ADDR=:9000
ENV S3LAZY_BACKEND=local
ENV S3LAZY_DATA_DIR=/data

EXPOSE 9000
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:9000/health || exit 1

CMD ["./s3lazy"]
