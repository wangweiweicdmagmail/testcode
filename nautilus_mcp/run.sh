#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT/.venv/bin/python3"
if [[ ! -x "$VENV_PY" ]]; then
  echo "MCP: .venv 不存在，请先运行: bash scripts/setup-mcp.sh" >&2
  exit 1
fi
exec "$VENV_PY" "$ROOT/nautilus_mcp/server.py"
