# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/hafo-agent ./cmd/hafo-coordinator ./cmd/hafo-router ./cmd/hafo-archiver

# One image, three binaries; each compose service picks its command.
FROM alpine:3.20 AS hafo
RUN adduser -D -u 10001 hafo
COPY --from=build /out/ /usr/local/bin/
USER hafo
EXPOSE 9100

# Backup, archive and restore tools: the stock server image plus mysqlbinlog,
# which that image leaves out. The binary is taken from MySQL's signed client
# RPM (the signature is checked against the key the image already trusts)
# instead of installing the package, which conflicts with the image's
# server-minimal package over shared client binaries.
FROM mysql:8.4.13 AS tools
RUN microdnf install -y cpio && microdnf clean all \
 && arch="$(uname -m)" && cd /tmp \
 && curl -fsSLo client.rpm "https://repo.mysql.com/yum/mysql-8.4-community/el/9/${arch}/mysql-community-client-8.4.13-1.el9.${arch}.rpm" \
 && rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-mysql && rpm -K client.rpm \
 && rpm2cpio client.rpm | cpio -idm ./usr/bin/mysqlbinlog \
 && install -m 0755 usr/bin/mysqlbinlog /usr/local/bin/mysqlbinlog \
 && rm -rf /tmp/client.rpm /tmp/usr && mysqlbinlog --version
COPY --from=build /out/hafo-archiver /usr/local/bin/
