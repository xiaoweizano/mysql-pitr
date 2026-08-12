#!/usr/bin/env bash
#
# MySQL PITR Agent - Install Script
# ====================================
# Usage:
#   curl -fsSL https://github.com/a-shan/mysql-pitr/releases/latest/download/install.sh | bash
#
# Environment variables:
#   PITR_VERSION   - Release version (default: latest)
#   PITR_DIR       - Install directory (default: /usr/local/bin)
#   PITR_CONFIG    - Config directory (default: /etc/agent)
#   INSTALL_SYSTEMD - Install systemd service if available (default: true)
#

set -euo pipefail

# ------------------------------------------------------------------
# Colors
# ------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

# ------------------------------------------------------------------
# Defaults
# ------------------------------------------------------------------
REPO="a-shan/mysql-pitr"
VERSION="${PITR_VERSION:-latest}"
INSTALL_DIR="${PITR_DIR:-/usr/local/bin}"
CONFIG_DIR="${PITR_CONFIG:-/etc/agent}"
INSTALL_SYSTEMD="${INSTALL_SYSTEMD:-true}"

# ------------------------------------------------------------------
# Detect OS and architecture
# ------------------------------------------------------------------
detect_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *)
      err "Unsupported architecture: $arch"
      exit 1
      ;;
  esac
}

detect_os() {
  local os
  os="$(uname -s)"
  case "$os" in
    Linux)  echo "linux" ;;
    Darwin) echo "darwin" ;;
    *)
      err "Unsupported OS: $os"
      exit 1
      ;;
  esac
}

# ------------------------------------------------------------------
# Resolve latest version from GitHub API
# ------------------------------------------------------------------
resolve_version() {
  if [ "$VERSION" = "latest" ]; then
    info "Resolving latest release version..."
    local api_url="https://api.github.com/repos/${REPO}/releases/latest"
    VERSION="$(curl -fsSL "$api_url" | grep '"tag_name":' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')"
    if [ -z "$VERSION" ]; then
      err "Could not resolve latest version. Check network or set PITR_VERSION."
      exit 1
    fi
    info "Latest version: ${VERSION}"
  fi
  # Strip leading 'v' for URL construction
  VERSION_NUM="${VERSION#v}"
}

# ------------------------------------------------------------------
# Download and install binary
# ------------------------------------------------------------------
download_and_install() {
  local os="$1"
  local arch="$2"
  local binary="mysql-pitr-agent"
  local archive="${binary}-${os}-${arch}.tar.gz"
  local url="https://github.com/${REPO}/releases/download/${VERSION}/${archive}"

  info "Downloading ${archive}..."
  curl -fsSL -o "/tmp/${archive}" "$url"

  info "Extracting..."
  tar -xzf "/tmp/${archive}" -C /tmp/

  info "Installing ${binary} to ${INSTALL_DIR}/..."
  if [ ! -d "$INSTALL_DIR" ]; then
    mkdir -p "$INSTALL_DIR"
  fi
  install -m 0755 "/tmp/${binary}" "${INSTALL_DIR}/${binary}"
  pass "Installed ${binary} to ${INSTALL_DIR}/${binary}"

  # Cleanup
  rm -f "/tmp/${archive}" "/tmp/${binary}"
}

# ------------------------------------------------------------------
# Create config directory
# ------------------------------------------------------------------
setup_config() {
  if [ ! -d "$CONFIG_DIR" ]; then
    info "Creating config directory: ${CONFIG_DIR}"
    mkdir -p "$CONFIG_DIR"
  fi

  # Create default config if it doesn't exist
  if [ ! -f "${CONFIG_DIR}/config.json" ]; then
    info "Creating default config at ${CONFIG_DIR}/config.json"
    cat > "${CONFIG_DIR}/config.json" <<-CONFEOF
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "replicator",
    "password": "change-me",
    "database": "mysql"
  },
  "server": {
    "url": "wss://pitr.example.com:9443/ws/agent",
    "cert_file": "/etc/agent/client.pem",
    "key_file": "/etc/agent/client-key.pem",
    "ca_file": "/etc/agent/ca.pem"
  },
  "data_dir": "/var/lib/mysql-pitr",
  "binlog_dir": ""
}
CONFEOF
    pass "Default config created"
  else
    info "Config already exists at ${CONFIG_DIR}/config.json, skipping"
  fi
}

# ------------------------------------------------------------------
# Install systemd service
# ------------------------------------------------------------------
install_systemd() {
  if [ "$INSTALL_SYSTEMD" != "true" ]; then
    info "Skipping systemd service installation (INSTALL_SYSTEMD=false)"
    return
  fi

  if ! command -v systemctl &>/dev/null; then
    warn "systemctl not found — skipping systemd service installation"
    return
  fi

  local unit_name="agent.service"
  local unit_path="/etc/systemd/system/${unit_name}"

  info "Installing systemd unit: ${unit_path}"

  # Find the unit file alongside this script, or download from releases
  local script_dir
  script_dir="$(cd "$(dirname "$0")" && pwd)"
  local local_unit="${script_dir}/${unit_name}"

  if [ -f "$local_unit" ]; then
    cp "$local_unit" "$unit_path"
  else
    info "Downloading systemd unit from GitHub..."
    local unit_url="https://github.com/${REPO}/releases/download/${VERSION}/${unit_name}"
    curl -fsSL -o "$unit_path" "$unit_url"
  fi

  chmod 0644 "$unit_path"

  info "Reloading systemd daemon..."
  systemctl daemon-reload

  info "Enabling and starting ${unit_name}..."
  systemctl enable "$unit_name"
  systemctl start "$unit_name"

  pass "Systemd service ${unit_name} installed and started"
}

# ------------------------------------------------------------------
# Print summary
# ------------------------------------------------------------------
print_summary() {
  echo ""
  echo "============================================"
  echo " MySQL PITR Agent Installation Complete"
  echo "============================================"
  echo " Binary:     ${INSTALL_DIR}/mysql-pitr-agent"
  echo " Config:     ${CONFIG_DIR}/config.json"
  echo " Version:    ${VERSION}"
  echo ""
  echo " Quick start:"
  echo "   mysql-pitr-agent --help"
  echo ""
  echo " To run as a daemon connected to the server:"
  echo "   mysql-pitr-agent serve --config=${CONFIG_DIR}/config.json --passphrase=<passphrase>"
  echo ""
  echo " For an offline one-shot flashback:"
  echo "   mysql-pitr-agent flashback --config=${CONFIG_DIR}/config.json --passphrase=<passphrase>"
  echo ""
  if command -v systemctl &>/dev/null && [ "$INSTALL_SYSTEMD" = "true" ]; then
    echo " Service status:"
    echo "   systemctl status agent"
  fi
  echo "============================================"
}

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
main() {
  echo ""
  echo " MySQL PITR Agent Installer"
  echo "=============================="
  echo ""

  # Root check
  if [ "$(id -u)" -ne 0 ]; then
    err "This script must be run as root (or with sudo)"
    exit 1
  fi

  local os arch
  os="$(detect_os)"
  arch="$(detect_arch)"
  info "Detected: ${os}/${arch}"

  resolve_version
  download_and_install "$os" "$arch"
  setup_config
  install_systemd
  print_summary
}

main "$@"
