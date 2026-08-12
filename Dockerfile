# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Frontend build
# =============================================================================
FROM node:20-alpine AS frontend

WORKDIR /web

COPY web/package*.json ./
RUN npm ci

COPY web/ .
RUN npm run build
# SvelteKit adapter-static output lands in /web/build (index.html, _app/,
# favicon.svg, robots.txt).

# =============================================================================
# Stage 2: Go build
# =============================================================================
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache dependency downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build agent binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /build/mysql-pitr-agent ./cmd/agent

# Embed the SvelteKit frontend into the server binary. The frontend is
# compiled in at build time via go:embed (internal/server/embed.go) — it is
# NOT shipped as files in the image. Without this step the server binary
# would serve the placeholder stub instead of the real UI.
COPY --from=frontend /web/build ./internal/server/embed_build/

# Build server binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /build/mysql-pitr-server ./cmd/server

# =============================================================================
# Stage 3: Agent image
# =============================================================================
# The v3 agent parses binlogs with go-mysql internally — no mysqlbinlog or
# mysql client tools are needed. python3/openssl/curl/jq stay for the
# provision service, which shares this image (scripts/e2e-provision.sh uses
# them to register the agent and issue its mTLS certificate).
FROM debian:12-slim AS agent

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tzdata python3 openssl curl jq && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/mysql-pitr-agent /usr/local/bin/mysql-pitr-agent

ENTRYPOINT ["mysql-pitr-agent"]
CMD []

# =============================================================================
# Stage 4: Server image
# =============================================================================
FROM alpine:3.20 AS server

# The server no longer parses binlogs itself — the agent does — so no
# mysql/mariadb client is needed.
RUN apk add --no-cache ca-certificates tzdata

# The SvelteKit frontend is already embedded in the server binary (see the
# builder stage above) — the image ships no separate web files.
COPY --from=builder /build/mysql-pitr-server /usr/local/bin/mysql-pitr-server

EXPOSE 8080 9443

ENTRYPOINT ["mysql-pitr-server"]
