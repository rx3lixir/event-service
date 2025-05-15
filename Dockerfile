# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o event-service ./cmd/event/main.go

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install dependencies
RUN apk --no-cache add ca-certificates tzdata

# Copy the binary from builder
COPY --from=builder /app/event-service .

# Copy any config files if needed
# COPY --from=builder /app/config ./config

# Expose the port
EXPOSE 9091

# Set entry point
CMD ["./event-service"]
