FROM golang:1.18-alpine AS builder

WORKDIR /app

COPY src/go.mod src/go.sum ./
RUN go mod download

COPY src/ .
COPY .env .

RUN CGO_ENABLED=0 GOOS=linux go build -a -o redis-metrics-exporter .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/redis-metrics-exporter .
COPY --from=builder /app/.env .

EXPOSE 9808

CMD ["./redis-metrics-exporter"]
