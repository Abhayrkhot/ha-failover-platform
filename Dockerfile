# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/hafo-agent ./cmd/hafo-agent

FROM alpine:3.20 AS agent
RUN adduser -D -u 10001 hafo
COPY --from=build /out/hafo-agent /usr/local/bin/hafo-agent
USER hafo
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/hafo-agent"]
