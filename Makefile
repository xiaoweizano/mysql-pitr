# Binary names
AGENT_BIN   := mysql-pitr-agent
SERVER_BIN  := mysql-pitr-server
OUTPUT_DIR  := bin

# Go parameters
GOCMD       := go
GOBUILD     := $(GOCMD) build
GOTEST      := $(GOCMD) test
GOMOD       := $(GOCMD) mod
GOLINT      := golangci-lint
GOVET       := $(GOCMD) vet

# Build flags
LDFLAGS     := -ldflags="-s -w"

.PHONY: all build agent server test lint vet clean docker-build fmt build-web

all: lint test build

# --- Build ---

build: agent server

agent:
	$(GOBUILD) $(LDFLAGS) -o $(OUTPUT_DIR)/$(AGENT_BIN) ./cmd/agent

server:
	$(GOBUILD) $(LDFLAGS) -o $(OUTPUT_DIR)/$(SERVER_BIN) ./cmd/server

# --- Test ---

test:
	$(GOTEST) -v -race -count=1 ./...

test-short:
	$(GOTEST) -short -count=1 ./...

test-race:
	$(GOTEST) -race -count=1 ./...

test-cover:
	$(GOTEST) -v -race -count=1 -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# --- Linting & Vet ---

lint:
	$(GOLINT) run ./...

vet:
	$(GOVET) ./...

# --- Clean ---

clean:
	rm -rf $(OUTPUT_DIR) coverage.out coverage.html

# --- Frontend ---

# Build the SvelteKit frontend and copy it into internal/server/embed_build/
# so it is compiled into the server binary (single-binary delivery, served via
# resolveWebFS). Run before `make server` / `go build ./cmd/server`.
# NOTE: `rm -rf …/*` deliberately keeps the dotfile .gitkeep (sh globs do not
# match it), so the directory stays git-trackable after builds.
build-web:
	cd web && npm ci && npm run build
	rm -rf internal/server/embed_build/*
	cp -r web/build/* internal/server/embed_build/

# --- Docker ---

docker-build:
	docker build -t $(AGENT_BIN):latest -f Dockerfile --target agent .
	docker build -t $(SERVER_BIN):latest -f Dockerfile --target server .

# --- Format ---

fmt:
	$(GOCMD) fmt ./...

# --- Dependencies ---

deps:
	$(GOMOD) tidy
	$(GOMOD) download

tidy:
	$(GOMOD) tidy
