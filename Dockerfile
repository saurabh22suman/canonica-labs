# Canonica Gateway Dockerfile
# Multi-stage build for minimal production image

# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files first for layer caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the gateway binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /canonica-gateway ./cmd/gateway

# Production stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates wget

# Copy binary from builder
COPY --from=builder /canonica-gateway /app/canonica-gateway

# Create non-root user
RUN adduser -D -g '' canonica
USER canonica

# Expose gateway port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run gateway
ENTRYPOINT ["/app/canonica-gateway"]
