FROM golang:1.25.3-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /workspace
COPY go.work go.work.sum ./
COPY services/telemetry-schemas/ services/telemetry-schemas/
COPY services/mock-generator/ services/mock-generator/
COPY services/edge-gateway/ services/edge-gateway/

WORKDIR /workspace/services/mock-generator
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /workspace/mock-generator-bin .

FROM alpine:3.19 AS runner

RUN apk add --no-cache ca-certificates \
    && addgroup -S chronos \
    && adduser -S chronosuser -G chronos

WORKDIR /app
COPY --from=builder /workspace/mock-generator-bin ./mock-generator
USER chronosuser:chronos
ENTRYPOINT ["./mock-generator"]
