# ---- Build Stage ----
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go module files first for better layer caching
COPY go.mod ./
# No go.sum since we use only standard library

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /aggregator .

# ---- Run Stage ----
FROM alpine:3.19

WORKDIR /app

# Copy binary from builder
COPY --from=builder /aggregator /app/aggregator

# Create results directory
RUN mkdir -p /app/results

ENTRYPOINT ["/app/aggregator"]
CMD ["--input", "/app/ad_data.csv", "--output", "/app/results/"]
