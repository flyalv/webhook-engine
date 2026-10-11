FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/webhook-engine ./cmd/server

FROM alpine:3.22

RUN apk --no-cache add ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

WORKDIR /app

COPY --from=builder /out/webhook-engine /app/webhook-engine

USER app

EXPOSE 8080

ENTRYPOINT ["/app/webhook-engine"]
