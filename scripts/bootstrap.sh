#!/usr/bin/env bash
# 一键启动 Alpha 栈（不含 OpenClaw、不含 IBKR 引擎）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export NAUTILUS_ROOT="$ROOT"
exec bash "$ROOT/scripts/start-stack.sh"
