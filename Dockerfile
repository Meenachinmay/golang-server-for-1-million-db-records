# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .

RUN go build -o main ./main.go

# Run stage
FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/main .

RUN chmod +x /app/main

EXPOSE 8080

CMD ["sh", "-c","/app/main"]