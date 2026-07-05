# 需求边界（固定，不扩展）

> **状态**：2026-07-05 冻结。本文档是测量 / 审计 / 生产护栏相关需求的**唯一范围说明**。  
> 未列入「范围内」的一律不做，除非新开需求并更新本文档。

---

## 一、范围内（已交付）

### 1. 全链路审计（Journal）

| 项 | 实现 | 存储 |
|----|------|------|
| 提案生命周期 | `proposal:update` → `journal.recordProposalUpdate` | `.run/journal.jsonl` |
| 信号 touch | `signal:touch` → `recordSignalTouch` | 同上 |
| 自动策略 | `auto:signal` → `recordAutoSignal` | 同上 |
| 订单 | `order:update` + `journal:coid:{coid}` 归因 | 同上 |
| 持仓 / 成交 | `position:update` → 快照 + `kind=trade` | 同上 |
| 查询 | `GET /api/journal/day`, `/timeline`, `/trades`, … | 内存索引 + JSONL |
| UI | `/audit.html` | 按 ET 日 + 提案时间线 |

### 2. 信号 outcome（SQLite）

| 项 | 实现 |
|----|------|
| touch 落库 | `signal_detector` → `measurement/signal_store.record_touch` |
| auto 落库 | `auto_runner._publish_signal` → `record_auto` |
| 库文件 | `.run/signals.db`（`SIGNAL_DB_PATH` 可改） |
| 回填脚本 | `python scripts/check_signal_outcomes.py` |
| 支持 outcome | **仅 touch**（有 `touch_time`，与 Redis bar 同坐标系） |
| SPY benchmark | 同上脚本，Redis `bars:1m:SPY` |

### 3. 启动对账

| 项 | 实现 |
|----|------|
| 触发 | 引擎启动后 ~25s，`auto_runner._recover` |
| 逻辑 | `recover_all` → `reconcile_startup`（broker 持仓 + cache 挂单 + Redis 单元） |
| 可读 | Redis `reconcile:startup`（24h TTL） |
| 周期对账 | 每根 M5 `auto_pm.reconcile(sym)`（原有，不变） |

### 4. OrderPolicy（延迟行情）

| 项 | 实现 |
|----|------|
| 模块 | `portfolio/order_policy.py`, `portfolio/ib_orders.py`, `portfolio/market_data.py` |
| 接入点 | `AutoPM` 入场/平仓；`order_actor` HTTP `/close` |
| 开关 | `MARKET_DATA_DELAYED=1` 或 `MARKET_DATA_MODE=delayed_frozen` → marketable LIMIT |
| Bracket | AutoPM 止损/止盈 `ocaGroup`+`ocaType=1`（IB OCA） |
| FA 校验 | 启动 `requestFA(GROUPS)` 验证 `IB_FA_GROUP` |

### 5. 测试与 CI

| 项 | 实现 |
|----|------|
| 单测 | `test_risk_gate`, `test_order_policy`, `test_signal_store`, `test_trading_env` |
| 验收 | `python scripts/verify_scope.py`（对照本文档静态检查） |
| CI | `.github/workflows/ci.yml`（无 Nautilus / 无 IBKR） |

### 6. 配置只读 UI

| 项 | 实现 |
|----|------|
| API | `GET /api/config/settings`（`.env` + Redis `config:auto` + 最近对账） |
| UI | `/settings.html` |
| 架构 / 使用说明 | `/docs.html` |
| 写入 | **无**。改配置 = 改 `.env` + 重启 `main.py` |

### 7. 上层 → 引擎参数边界（2026-07-05 冻结）

| 项 | 规格 |
|----|------|
| 主下单路径 | Alpha 审批 → `TradeIntent` → `RiskGate` → `AutoPM`（**唯一**实盘开仓路径） |
| 手动开仓 | 前端 / `order_actor` HTTP `/order` **禁止**（403） |
| AutoPM 平仓 | `settings.auto_strategy` 或 `opening_breakout_live` 的标的，UI/API 平仓 **不得** 直调 `order_actor /close` |
| 平仓路由 | `DELETE /api/position` → Redis `auto:close:{sym}` → `AutoRunner` → `AutoPM.close_all` |
| 手动/trail 平仓 | 无 AutoPM 接管时 → 仍走 `order_actor` `/close` |
| 改止损 | AutoPM 接管标的 **禁止** `/modify-stop`（409）；trail 仅 manual 仓位 |
| 幽灵仓位 | `POST /api/position` **禁止**（403），`position:*` 仅引擎写入 |
| Agent 定量 | 执行时用 **当前 M1 close** 刷新 `ref_price` 再 `RiskGate`（stop/tp 仍用 proposal） |
| FA / 行情 | 上层不传；`main.py` env → `AutoRunner` / `order_actor` |
| 标的范围 | proposal `symbol` 须在 `GATEWAY_INSTRUMENTS`，否则引擎 `instrument_not_loaded` |

---

## 二、明确不做（范围外）

以下**不在当前需求内**，审核时若提出一律拒绝或另开文档：

- IBKR historical 拉取 / 7 日 outcome 自动补全（Redis 无 bar 则 skip，接受）
- `auto:signal` 的 outcome 回填（无 `touch_time`，设计如此）
- LIMIT 入场未成交超时 / 撤单重试
- Settings Web **热修改** / 写回 `.env`
- Journal 哈希链 / PostgreSQL 迁移
- Prometheus / Grafana
- 启动对账改为主动 heartbeat 握手（当前 25s Timer 固定）
- 引擎内 cron 扫信号（仍靠 MCP / 外部 cron）
- 前端恢复手动 Bracket / 限价开仓 UI 能力
- MessageBus 跨进程（Node↔Python）；平仓路由固定 Redis `auto:close:{sym}`
- Settings Web 写回 `.env` / FA Group 热改

---

## 三、已知限制（接受，非待办）

| 限制 | 说明 |
|------|------|
| 平仓 PnL | `journal` 的 `trade` 来自持仓快照估算（`pnl_source: snapshot_estimate`） |
| outcome 7d | 依赖 Redis `bars:1m:*` 仍保留目标时点 K 线；否则脚本 skip |
| 时间轴 | touch 用 ET fake-UTC（`touch_time`）；`created_at` 仅用于判断信号年龄 |
| `avg_alpha_vs_spy_7d` | 方向调整后的信号收益 − SPY 绝对收益，**非**严格 alpha 定义 |
| 启动 25s | cache 未就绪时记 `entry_pending_cache`，不 void 单元 |
| CI 范围 | 不跑 `test_auto_pm` 等依赖 Nautilus 的测试 |
| UI 平仓 | AutoPM 标的走 Redis 路由，成交后清 UI（非立即 reload） |

---

## 四、日常命令（固定）

```bash
# 栈
bash scripts/start-stack.sh
python main.py

# 收盘后
# 审计：浏览器 /audit.html 或 GET /api/journal/day?date=YYYY-MM-DD
# outcome（touch 信号，7 日龄+）
python scripts/check_signal_outcomes.py
# 配置查看
open http://localhost:3000/settings.html
# 架构与使用说明
open http://localhost:3000/docs.html

# 单测
pytest scripts/test_risk_gate.py scripts/test_order_policy.py scripts/test_signal_store.py -q
python scripts/test_trading_env.py
# 规格验收（冻结范围）
python scripts/verify_scope.py
```

---

## 五、相关文件索引

```
measurement/signal_store.py
scripts/check_signal_outcomes.py
portfolio/order_policy.py
execution/auto_pm.py          # reconcile_startup, OrderPolicy
portfolio/auto_settings.py
frontend/server.js            # DELETE position 路由、modify-stop 拦截
auto_runner.py                # _drain_close_request、ref_price 刷新
frontend/journal.js
frontend/public/audit.html
frontend/public/settings.html
.github/workflows/ci.yml
.run/journal.jsonl            # gitignore
.run/signals.db               # gitignore
```

---

## 变更规则

1. 新功能必须先写入本文档「范围内」并标版本日期，再写代码。  
2. 「范围外」项不得顺手实现。  
3. Bugfix 可改实现，不得扩大行为（例如 outcome 仍只覆盖 touch）。
