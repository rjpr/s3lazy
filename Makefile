.PHONY: test-unit test-all coverage coverage-html build clean help

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build:  ## Build the binary
	go build -o s3lazy .

test-unit:  ## Run unit tests (no Docker required)
	go test -v ./...

test-all:  ## Run all tests (unit + integration)
	go test -tags=integration -timeout 5m -v ./...

coverage:  ## Run tests with coverage report
	go test -tags=integration -coverprofile=coverage.out -timeout 5m ./...
	go tool cover -func=coverage.out

coverage-html:  ## Generate HTML coverage report
	go test -tags=integration -coverprofile=coverage.out -timeout 5m ./...
	go tool cover -html=coverage.out -o coverage.html

clean:  ## Clean build artifacts
	rm -f s3lazy coverage.out coverage.html
