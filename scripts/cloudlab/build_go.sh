#!/usr/bin/env bash
# Install Go on this node if missing and build the Go programs into bin/.
#
# Settings: GO_VERSION [1.23.12], GO_HOME [/opt/go].
set -euo pipefail

GO_VERSION=${GO_VERSION:-1.23.12}
GO_HOME=${GO_HOME:-/opt/go}

if [[ ! -x $GO_HOME/bin/go ]]; then
  tarball=/tmp/go$GO_VERSION.tar.gz
  curl -fsSL -o "$tarball" "https://go.dev/dl/go$GO_VERSION.linux-$(dpkg --print-architecture).tar.gz"
  sudo -n mkdir -p "$GO_HOME"
  sudo -n chown "$(id -un):" "$GO_HOME"
  tar -xzf "$tarball" -C "$GO_HOME" --strip-components=1
fi

cd "$(dirname "$0")/../.."
CGO_ENABLED=0 "$GO_HOME/bin/go" build -o bin/ ./cmd/...
