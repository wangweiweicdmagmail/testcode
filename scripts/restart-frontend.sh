#!/usr/bin/env bash
# 重启前端（清理 3000 端口 + 健康检查）
# 等价于在项目根目录: node frontend/server.js
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "$ROOT/scripts/stop-stack.sh" 2>/dev/null || true
if lsof -ti :3000 >/dev/null 2>&1; then
  echo "[frontend] 清理占用 3000 的进程 ..."
  lsof -ti :3000 | xargs kill -9 2>/dev/null || true
  sleep 1
fi
bash "$ROOT/scripts/start-stack.sh"
