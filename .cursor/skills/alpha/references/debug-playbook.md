# Alpha 调试手册（Agent 深查用）

## 前置检查清单

| 检查项 | MCP / CLI | 正常 | 异常处理 |
|--------|-----------|------|----------|
| Redis | `get_stack_health` | `redis_ok: true` | 启动 redis / `start-stack.sh` |
| 引擎 | `health.engine_heartbeat` 非 null | 有 timestamp | `python main.py` |
| M1  bars | `get_m1_bars` | `count > 0`，含 `vwap` | 等 flush ~60s 或重启引擎 |
| M5 active | `get_indicators_active_tool` | `supertrend.dir` ∈ {1,-1} | 等 M5 收盘或 `audit_m5_st` |
| 触线 | `list_recent_touches_tool` | 盘中应有事件 | 确认 `SignalDetector` 在 main.py |
| 前端审批 | http://localhost:3000/proposals.html | 可打开 | `node frontend/server.js` |

## 触线类型与水平线来源

| signal_type | 水平线 | 顺势判断 |
|-------------|--------|----------|
| `pullback_vwap` | M1 session VWAP | M5 ST dir |
| `pullback_supertrend` | M5 冻结 ST（`indicators:active`） | M5 ST dir |
| `pullback_dema20` | M5 冻结 DEMA20 | M5 ST dir |

LONG：仅当 `m5_st_dir == 1`；SHORT：仅当 `m5_st_dir == -1`。

## 扫描过滤链（顺序）

```
signals:touch:index 候选
  → touch_too_old（超出 ALPHA_TOUCH_MAX_AGE_SECONDS）
  → counter_trend（side vs m5_st_dir）
  → pending_same_side
  → duplicate_proposal（proposal:dedup）
  → pricing_failed（结构止损算不出）
  → rr_below_min
  → 按 confidence 排序
  → symbol_cap / scan_cap
  → store_pending
```

## skip / purge / create 原因码

### `run_alpha_scan` → skipped.reason

| reason | 含义 | 用户可读 |
|--------|------|----------|
| `touch_too_old` | 触线早于「最新 M1 - max_age」 | 触线过旧，本轮不建 |
| `counter_trend` | side 与 M5 ST 相反 | 逆势，已过滤 |
| `pending_same_side` | 同标的同向已有 pending | 等审批或先 purge |
| `duplicate_proposal` | dedup key 已存在 | 该触线已建过 |
| `pricing_failed` | pullback 定价失败 | K 线不足或结构无效 |
| `rr_below_min` | 半仓 R:R < ALPHA_MIN_RR_HALF | R:R 不够 |
| `symbol_cap` | 本轮该标的已达上限 | 每标的≤1 |
| `scan_cap` | 全市场本轮已达上限 | 全市场≤3 |

### `purge_pending_proposals` → purged.reason

| reason | 含义 |
|--------|------|
| `expired` | `expires_at` 已过 |
| `touch_stale` | 触线超出清理窗口 |
| `counter_trend` | 与当前 M5 ST 逆势 |

### `create_proposal` → error

| error | hint |
|-------|------|
| `symbol_excluded` | 不在 ALPHA_SYMBOLS |
| `touch_not_found` | touch_time/side/type 与 Redis 不匹配 |
| 同上 skip 码 | 同 incremental 过滤 |

## 备选入口对照

| 场景 | 推荐 | 备选 1 | 备选 2 |
|------|------|--------|--------|
| 日常 cron | MCP `run_alpha_scan` | — | — |
| MCP 不可用 | `python scripts/scan_signals.py` | 内联 Python `run_incremental_scan` | — |
| 只看状态 | MCP `get_alpha_snapshot` | `get_stack_health` | `python scripts/health_check.py` |
| 清 pending | MCP `purge_pending_proposals` | `purge` dry_run 先预览 | 前端批量驳回 |
| 单条补建 | MCP `create_proposal` | — | — |
| ST 对账 | MCP `audit_m5_st` | `python scripts/test_st_flip.py` | — |
| 全量回放调试 | `scan_signals.py --full` | **禁止 cron** | — |
| Legacy  daemon | `python alpha_agent.py` | 需 DEEPSEEK | 默认关闭 |

## CLI 速查

```bash
# 增量扫描（同 MCP run_alpha_scan）
python scripts/scan_signals.py

# 调试：24h 触线回放（勿 cron）
python scripts/scan_signals.py --full

# 列 pending
python scripts/list_proposals.py --status pending

# 审批
python scripts/approve_proposal.py --id <id> --decision rejected

# 栈健康
python scripts/health_check.py
python nautilus_mcp/self_test.py
```

## ST / VWAP 异常

| 现象 | 工具 | 处理 |
|------|------|------|
| M5 ST 方向与图表不符 | `audit_m5_st` | 重启 `main.py` 重刷 Redis |
| VWAP 11:45 后跳变 | 查 `bars:1m` 连续 vwap | 已修 poll 双喂，需重启引擎 |
| indicators:active 与 bars:5m 不一致 | `audit_m5_st` | 同上 |

## 审批之后（引擎内，非 MCP）

```
pending → 人工批准 → approved_wait
  → ReclaimWatcher（M1 reclaim）→ ready_to_execute
  → AutoRunner（M1）→ 下单
```

MCP **不得**改 `approved` / 直接下单。
