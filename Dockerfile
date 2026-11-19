# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/hafo-agent ./cmd/hafo-coordinator ./cmd/hafo-router

# One image, three binaries; each compose service picks its command.
FROM alpine:3.20 AS hafo
RUN adduser -D -u 10001 hafo
COPY --from=build /out/ /usr/local/bin/
USER hafo
EXPOSE 9100
