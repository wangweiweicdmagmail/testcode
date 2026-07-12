#!/usr/bin/env bash
# 启动监控器：一条命令自启 Redis + 引擎 + 前端，按序探测 8 个关键步骤，每步 macOS say 播报，
# 卡住则 afplay 告警 + 给出指引。引擎后台拉起（日志 → .run/logs/launch_engine.log，`tail -f` 看）。
#
# 用法：
#   bash scripts/launch.sh                 # 默认起全部（Redis + 引擎 + 前端 + 开浏览器）
#   bash scripts/launch.sh --no-engine     # 引擎改手动起（你另开终端 python main.py，保留实时日志）
#   bash scripts/launch.sh --symbol NVDA --no-browser
#   PYTHON=/opt/anaconda3/bin/python3 bash scripts/launch.sh   # 指定 Python 解释器
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

# ── 参数 ────────────────────────────────────────────────────────
SYMBOL="${SYMBOL:-QQQ}"
IBG_HOST="${IBG_HOST:-127.0.0.1}"
IBG_PORT="${IBG_PORT:-7496}"
PYTHON="${PYTHON:-python3}"
SAY_VOICE="${SAY_VOICE:-Tingting}"      # 中文女声；缺省回退默认 say
START_ENGINE=1   # 默认连带后台起 main.py；--no-engine 改为手动起（保留实时日志）
OPEN_BROWSER=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-engine) START_ENGINE=0; shift ;;
    --start-engine) START_ENGINE=1; shift ;;   # 向后兼容（已是默认）
    --no-browser) OPEN_BROWSER=0; shift ;;
    --symbol) SYMBOL="${2:-$SYMBOL}"; shift 2 ;;
    *) shift ;;
  esac
done

RUN_DIR="$ROOT/.run"
LOG_DIR="$ROOT/.run/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"
ENGINE_LOG="$LOG_DIR/launch_engine.log"

# ── 工具 ────────────────────────────────────────────────────────
log() { echo "[$(date +%H:%M:%S)] $*"; }

# 后台语音播报（不阻塞主流程）
speak() {
  local msg="$1"
  log "🔊 $msg"
  ( say -v "$SAY_VOICE" "$msg" 2>/dev/null || say "$msg" 2>/dev/null ) &
  disown 2>/dev/null || true
}

# 告警音 + 语音
alert() {
  local msg="$1"
  log "⚠️ $msg"
  ( afplay /System/Library/Sounds/Basso.aiff 2>/dev/null
    say -v "$SAY_VOICE" "$msg" 2>/dev/null || say "$msg" 2>/dev/null ) &
  disown 2>/dev/null || true
}

# wait_for <timeout_s> <probe_cmd...>：探测成功返回 0，超时返回 1
wait_for() {
  local timeout=$1; shift
  local elapsed=0
  while ! "$@" >/dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    (( elapsed >= timeout )) && return 1
  done
  return 0
}

# ── 探测函数 ────────────────────────────────────────────────────
probe_redis()      { redis-cli ping 2>/dev/null | grep -q PONG; }
probe_tws()        { nc -z -w 2 "$IBG_HOST" "$IBG_PORT" 2>/dev/null; }
probe_engine_pid() { [[ -f "$ROOT/.engine.pid" ]] && kill -0 "$(cat "$ROOT/.engine.pid" 2>/dev/null)" 2>/dev/null; }
probe_heartbeat()  { [[ -n "$(redis-cli get engine:heartbeat 2>/dev/null)" ]]; }
probe_account()    { redis-cli exists account:funds 2>/dev/null | grep -q 1; }
probe_bar()        { local n; n="$(redis-cli llen "bars:1m:$SYMBOL" 2>/dev/null)"; [[ "$n" =~ ^[0-9]+$ && "$n" -gt 0 ]]; }
probe_indicators() {
  local d
  d="$(redis-cli get "indicators:active:$SYMBOL" 2>/dev/null | "$PYTHON" -c "import json,sys; d=json.load(sys.stdin); print(d.get('supertrend',{}).get('dir',0))" 2>/dev/null)"
  [[ "$d" == "1" || "$d" == "-1" ]]
}
probe_frontend()   { curl -sf -o /dev/null --max-time 2 "http://127.0.0.1:3000/console.html" 2>/dev/null; }

# ── 自启 Redis / 前端 ───────────────────────────────────────────
ensure_redis() {
  if probe_redis; then log "redis 已运行"; return 0; fi
  log "启动 redis-server ..."
  command -v redis-server >/dev/null 2>&1 && redis-server --daemonize yes >/dev/null 2>&1 || true
  sleep 1
  probe_redis
}

ensure_frontend() {
  if probe_frontend; then log "frontend 已运行"; return 0; fi
  log "启动 frontend ..."
  if lsof -ti :3000 >/dev/null 2>&1; then lsof -ti :3000 | xargs kill -9 2>/dev/null || true; sleep 1; fi
  nohup "$ROOT/scripts/run-frontend.sh" >>"$LOG_DIR/frontend.log" 2>&1 &
  disown 2>/dev/null || true
  sleep 2
  probe_frontend
}

ensure_engine() {
  if probe_engine_pid; then log "引擎进程已在运行"; return 0; fi
  if [[ "$START_ENGINE" != 1 ]]; then return 1; fi
  log "后台启动引擎 → $ENGINE_LOG"
  nohup "$PYTHON" "$ROOT/main.py" >>"$ENGINE_LOG" 2>&1 &
  disown 2>/dev/null || true
  return 0
}

# ── 主序列 ──────────────────────────────────────────────────────
log "=== 交易台启动监控 === symbol=$SYMBOL  tws=$IBG_HOST:$IBG_PORT  engine=$([[ $START_ENGINE == 1 ]] && echo self-start || echo manual)"

# 1. Redis
if ensure_redis; then speak "Redis 就绪"; else alert "Redis 启动失败，请运行 brew services start redis"; exit 1; fi

# 2. TWS / IB Gateway API 端口
log "探测 TWS API $IBG_HOST:$IBG_PORT ..."
if wait_for 5 probe_tws; then speak "TWS 已连接"; else
  alert "未连上 TWS，请确认 TWS 或 IB Gateway 已启动并开启 API 端口 $IBG_PORT"
  log "（继续等待引擎，但 IBKR 连接可能失败）"
fi

# 3. 引擎进程
if [[ "$START_ENGINE" == 1 ]]; then ensure_engine; fi
log "等待引擎进程（.engine.pid）..."
if wait_for 180 probe_engine_pid; then speak "引擎进程已启动"
else
  if [[ "$START_ENGINE" != 1 ]]; then
    alert "引擎未启动，请在另一终端运行：cd $ROOT && python main.py"
    log "（启动后本监控会继续，等待心跳...）"
  else
    alert "引擎进程未出现，查看日志：tail -50 $ENGINE_LOG"; exit 1
  fi
fi

# 4. 引擎心跳（engine:heartbeat）
log "等待引擎心跳 ..."
if wait_for 120 probe_heartbeat; then speak "引擎在线"; else alert "引擎心跳未出现，可能 IBKR 连接失败，查看 main.py 日志"; exit 1; fi

# 5. 账户数据（IBKR reqAccountSummary，FA Group 约 3 分钟刷新）
log "等待账户数据 account:funds ..."
if wait_for 200 probe_account; then speak "账户数据已同步"; else alert "账户数据未到，检查 IBKR 授权与 FA Group 配置"; fi

# 6. 首根 M1 行情
log "等待 $SYMBOL 首根 M1 bar ..."
if wait_for 180 probe_bar; then speak "$SYMBOL 行情已接入"; else alert "$SYMBOL 行情未到，检查数据订阅"; fi

# 7. 指标就绪（ATR 预热 ~14 根 M1）
log "等待 $SYMBOL 指标就绪（supertrend.dir）..."
if wait_for 360 probe_indicators; then speak "$SYMBOL 指标就绪"; else alert "$SYMBOL 指标未就绪，预热不足或 Redis 无值"; fi

# 8. 前端
if ensure_frontend; then speak "前端就绪，可以开始"; else alert "前端启动失败，查看 $LOG_DIR/frontend.log"; exit 1; fi

log "=== 启动完成 ==="
if [[ "$OPEN_BROWSER" == 1 ]]; then
  open "http://localhost:3000/console.html"
  log "已打开浏览器 → console.html"
fi
speak "启动完成，控制台已就绪"
log "停止：bash scripts/stop-stack.sh（引擎 main.py 需单独 Ctrl-C）"
