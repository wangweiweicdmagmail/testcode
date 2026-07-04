#!/usr/bin/env bash
# 创建 .venv 并安装 MCP Server 依赖（Python 3.11，避免 3.14 与 mcp 不兼容）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${PYTHON_FOR_MCP:-}"
if [[ -z "$PY" ]]; then
  for c in python3.11 python3.12 python3; do
    if command -v "$c" >/dev/null 2>&1; then
      PY="$c"
      break
    fi
  done
fi
if [[ -z "$PY" ]]; then
  echo "[setup-mcp] 未找到 python3" >&2
  exit 1
fi

if [[ -d .venv ]]; then
  ver="$(.venv/bin/python3 -c 'import sys; print(sys.version_info[:2])' 2>/dev/null || echo skip)"
  if [[ "$ver" == "(3, 14)"* ]] || [[ "$ver" == "skip" ]]; then
    echo "[setup-mcp] 重建 .venv（当前 Python 不适用 MCP）"
    rm -rf .venv
  fi
fi

if [[ ! -d .venv ]]; then
  "$PY" -m venv .venv
  echo "[setup-mcp] 已创建 .venv ($("$PY" --version))"
fi

.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r nautilus_mcp/requirements.txt

echo "[setup-mcp] 完成。Cursor MCP 使用 nautilus_mcp/run.sh 启动。"
echo "  验证: .venv/bin/python3 nautilus_mcp/self_test.py"
