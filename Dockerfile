FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o s3lazy .

FROM alpine:latest

# Install ca-certificates for HTTPS and create non-root user
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 1000 s3lazy && \
    mkdir -p /data && \
    chown s3lazy:s3lazy /data

USER s3lazy
WORKDIR /app
COPY --from=builder /app/s3lazy .

EXPOSE 9000
VOLUME ["/data"]

CMD ["./s3lazy"]
