# Build stage
FROM golang:1.23-alpine AS builder

# Set working directory for builder
WORKDIR /app

# Copy source code
COPY . .

# Build consumer
WORKDIR /app/cmd/consumer
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o consumer .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy both binaries
COPY --from=builder /app/cmd/consumer/consumer .

EXPOSE 8080

# Use environment variable to determine which binary to run
CMD ["./consumer"]
