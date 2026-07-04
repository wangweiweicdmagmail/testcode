#!/usr/bin/env bash
# 前台运行前端（供 nohup / launchd 调用）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
exec node frontend/server.js
