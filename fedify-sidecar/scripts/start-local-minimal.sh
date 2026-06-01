#!/usr/bin/env sh
# Starts the sidecar in minimal mode (no RedPanda, no OpenSearch, no topic bootstrap)
# Loads .env.local from the project root
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ENV_FILE="$ROOT_DIR/.env.local"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Copy .env.local.example to .env.local first."
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

cd "$ROOT_DIR"
exec node --import tsx src/index.ts
