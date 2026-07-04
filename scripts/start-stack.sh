#!/usr/bin/env bash
# 启动策略栈（不含 main.py 引擎 — 需 IBKR TWS）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

RUN_DIR="$ROOT/.run"
LOG_DIR="$ROOT/.run/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"

log() { echo "[stack] $*"; }

is_running() {
  local name="$1"
  local pidfile="$RUN_DIR/$name.pid"
  [[ -f "$pidfile" ]] || return 1
  local pid
  pid="$(cat "$pidfile")"
  kill -0 "$pid" 2>/dev/null
}

start_bg() {
  local name="$1"
  shift
  if is_running "$name"; then
    log "$name 已在运行 (pid $(cat "$RUN_DIR/$name.pid"))"
    return 0
  fi
  log "启动 $name ..."
  nohup "$@" >>"$LOG_DIR/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/$name.pid"
  log "$name pid=$(cat "$RUN_DIR/$name.pid") log=$LOG_DIR/$name.log"
}

frontend_healthy() {
  curl -sf -o /dev/null --max-time 2 "http://127.0.0.1:3000/" 2>/dev/null
}

stop_one() {
  local name="$1"
  local pidfile="$RUN_DIR/$name.pid"
  [[ -f "$pidfile" ]] || return 0
  local pid
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    log "停止旧 $name pid=$pid"
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pidfile"
}

start_frontend() {
  if frontend_healthy; then
    log "frontend 已在运行 → http://localhost:3000"
    return 0
  fi
  stop_one frontend
  if lsof -ti :3000 >/dev/null 2>&1; then
    log "端口 3000 被占用，清理旧进程 ..."
    lsof -ti :3000 | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
  log "启动 frontend ..."
  nohup "$ROOT/scripts/run-frontend.sh" >>"$LOG_DIR/frontend.log" 2>&1 &
  disown $! 2>/dev/null || true
  echo $! >"$RUN_DIR/frontend.pid"
  log "frontend pid=$(cat "$RUN_DIR/frontend.pid") log=$LOG_DIR/frontend.log"
  sleep 2
  if ! frontend_healthy; then
    log "错误: frontend 启动失败，最近日志:"
    tail -8 "$LOG_DIR/frontend.log" >&2 || true
    exit 1
  fi
  log "frontend 健康检查通过 → http://localhost:3000"
}

# Redis
if redis-cli ping >/dev/null 2>&1; then
  log "redis 已运行"
else
  log "启动 redis-server ..."
  if command -v redis-server >/dev/null 2>&1; then
    redis-server --daemonize yes || true
    sleep 1
  fi
  if ! redis-cli ping >/dev/null 2>&1; then
    log "警告: Redis 未就绪，请先 brew services start redis"
  fi
fi

# 前端 API + 飞书 webhook
start_frontend

# 飞书推送（仅当 .env 已配置）
if python3 -c "
from pathlib import Path
import sys
sys.path.insert(0, '$ROOT')
from feishu.config import enabled
sys.exit(0 if enabled() else 1)
" 2>/dev/null; then
  start_bg feishu_notifier python3 "$ROOT/feishu/notifier.py"
else
  log "跳过 feishu_notifier（未配置 FEISHU_APP_ID/SECRET/RECEIVE_ID）"
fi

# Alpha 扫描（legacy：独立 Python + 可选 DeepSeek；推荐改用 Cursor MCP，见 docs/MCP_ALPHA.md）
if [[ "${ALPHA_USE_DAEMON:-0}" == "1" ]]; then
  start_bg alpha_agent python3 "$ROOT/alpha_agent.py"
else
  log "跳过 alpha_agent（默认用 Cursor MCP + skill nautilus-alpha；设 ALPHA_USE_DAEMON=1 启用 legacy）"
fi

log "完成。引擎请手动: cd $ROOT && python main.py"
log "健康检查: python scripts/health_check.py"
log "手动扫一轮: python scripts/scan_signals.py"
log "停止: bash scripts/stop-stack.sh"
