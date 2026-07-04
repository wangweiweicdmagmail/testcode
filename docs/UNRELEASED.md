# 未提交功能总结（Working Tree）

> **状态**：截至 2026-07-04，以下功能均在本地工作区，**尚未 git commit**。  
> **主策略产品名**：**超级信号**（`st_super`）  
> **核心原则**：人工审批不可跳过；自动执行仅走 `AutoRunner` 审批路径。

---

## 一、端到端流程（当前目标架构）

```text
IBKR → strategy.py（K线 + ST 指标写 Redis）
         ↓ bar.collected / bar.collected.m5
       SignalDetector（st_super：5m 定方向 + 1m 翻回对齐）
         ↓ signals:touch:{SYM}
       MCP alpha / scripts/scan_signals.py（增量扫描）
         ↓ proposal:pending
       Web multi.html / 飞书卡片 → 人工批准
         ↓ proposal:approved（st_super_immediate → ready_to_execute）
       AutoRunner（M1 消费）→ RiskGate → AutoPM → IBKR
```

| 阶段 | 关键模块 | Redis / 事件 |
|------|----------|--------------|
| K 线 | `strategy.py` | `bars:1m/5m`, `kline:*` |
| 指标 | `strategy.py` M1/M5 ST | `indicators:active`（M5 冻结） |
| 信号 | `signals/st_super.py`, `signal_detector.py` | `signals:touch:*`, PUBLISH `signal:touch` |
| 建议 | `approval/alpha_scan.py`, `proposal_builder.py` | `proposal:pending:*` |
| 审批 | `frontend/server.js`, `feishu/` | `proposal:approved:*` |
| 执行 | `auto_runner.py`, `execution/auto_pm.py` | `auto:units:*`, `auto:signal` |

---

## 二、超级信号（st_super）

### 规则

- **5m ST (10, 3.5)** 定方向；**1m ST (10, 3.0)** 翻回同向 → 入场
- **止损** = 翻转 K 的 1m ST 值；**止盈** = 默认 2R（半仓 TP + 余仓移保本）
- **入场窗口**：09:45–15:30 ET（与 `RiskGate` 一致）
- **执行模式**：`st_super_immediate`（批准后直接 `ready_to_execute`，跳过 Reclaim 等待）

### 后端

| 文件 | 作用 |
|------|------|
| `signals/st_super.py` | ST 状态机、翻转检测、定价、历史 warmup |
| `signal_detector.py` | M1/M5 订阅；emit touch；flush 后 replay st_super |
| `approval/proposal_builder.py` | 构建 pending 载荷 |
| `approval/alpha_scan.py` | 扫描过滤、`ALPHA_PRIMARY_SIGNAL=st_super` |
| `auto_runner.py` | M1 消费已批准建议 |
| `portfolio/risk_gate.py` | st_super 用 `size_by_stop` 定量 |

### 环境变量（见 `env.example`）

```bash
ALPHA_PRIMARY_SIGNAL=st_super
ALPHA_SUPER_ONLY=1              # 禁用 legacy M5 / 开盘突破 bypass
ST_SUPER_MULT_1M=3.0
ST_SUPER_MULT_5M=3.5
ST_SUPER_TP_RR=2.0
ALPHA_EMIT_PULLBACK_TOUCHES=0   # 默认不写回踩 touch
AUTO_FIXED_QTY=0                # 按 R 风控 sizing
```

### ST 参数统一

- `main.py` / `strategy.py` / `st_super.py` / 前端图表均使用 **M1=3.0, M5=3.5**
- 引擎重启后 `SignalDetector` 从 Redis `bars:5m/1m` **预热** st5 状态并 replay 当日 st_super touch

---

## 三、审批与治理

### 必须人工批准

- 生产路径：`pending` → `approved_live` → `AutoRunner` 下单
- **`ALPHA_SUPER_ONLY=1`**：关闭 `use_legacy_alpha`、`opening_breakout_*` 引擎内 bypass

### API 安全（`frontend/server.js`）

| 项 | 说明 |
|----|------|
| `NAUTILUS_BIND_HOST` | 默认 `127.0.0.1` |
| `NAUTILUS_API_SECRET` | 审批 / 取消 / 下单需 `X-Nautilus-Token` |
| `ORDER_GATEWAY_SECRET` | 代理引擎 8888 时携带 |
| 引擎离线 | **禁止批准**（防重启后意外自动执行） |
| 手动开仓 | 有待审批/待执行建议时 **403**（`server.js` + `order_actor.py` 双检） |
| MCP `create_proposal` | 尊重 `ALPHA_PRIMARY_SIGNAL`，不可绕过建回踩单 |

### 去重与边界

- pending **+ approved** 同方向不可重复建单
- 执行 claim TTL **600s**；observe 模式标记 `result=observed`
- 扫描 touch 时效默认 **300s**（需 cron/MCP 定期 `run_alpha_scan`）

---

## 四、前端（四宫格 `multi.html`）

### 超级信号展示

- ST 5m/1m 线：前端重算绘制（参数与后端一致）
- **标记 / pill / 语音**：以 Redis `signals:touch`（`st_super`）为准，非纯客户端 ST

### 语音播报

| 事件 | 文案示例 |
|------|----------|
| 新建议 | `NVDA 新建议多待审批` |
| 超级信号 | `NVDA 超级多` |
| 换格后补播 | reload 后从 `sessionStorage` 恢复 |

由 Status Bar 铃铛开关控制；WS `proposal:update` + 15s 轮询双通道。

### 四宫格自动换槽

新信号/新建议若不在当前四格：

1. 按**保护分**选最低分格子替换（持仓 / Alpha 待办 / 实盘 auto → 不可替换）
2. 手动选股、`observe`、刚换入的格子保护分更高
3. 替换后 reload 并 `?focus=SYM` 聚焦

元数据：`localStorage.multiSlotMeta`、`multiSymbols`。

### Alpha 审批 UI

- `shared/alpha-cell.js`：步骤条、批准/驳回、pill 优先级（有待办时覆盖超级信号 pill）
- 页面：`proposals.html`、`decisions.html`、`performance.html`

---

## 五、MCP Alpha（Cursor）

| 路径 | 说明 |
|------|------|
| `nautilus_mcp/server.py` | MCP 工具：`run_alpha_scan`、`get_stack_health` 等 |
| `.cursor/mcp.json` + `scripts/setup-mcp.sh` | Cursor 集配置 |
| `.cursor/skills/alpha/` | 快捷触发 `alpha` / 扫信号 |
| `scripts/scan_signals.py` | CLI 增量扫描（与 MCP 同逻辑） |

日常：引擎运行 → Cursor 发 `alpha` 或 cron 调 `scan_signals.py` → 前端/飞书审批。

---

## 六、飞书

| 组件 | 说明 |
|------|------|
| `feishu/notifier.py` | 订阅 `proposal:update`，新 pending 推卡片 |
| `frontend/server.js` | `/api/feishu/webhook` 卡片按钮 → 同 Web 审批 |
| `gateway/` | 飞书 IM → Cursor Agent 对话（可选） |
| `scripts/push_proposal_feishu.py` | 手动推卡片 |
| `scripts/list_feishu_chats.py` | 查 chat_id |

配置见 [FEISHU_SETUP.md](./FEISHU_SETUP.md)。

---

## 七、AutoRunner 三层架构

| 层 | 模块 |
|----|------|
| Alpha | `SignalDetector`（信号）+ MCP 扫描（建议） |
| Portfolio | `portfolio/risk_gate.py`（时段、熔断、冷却、以损定量） |
| Execution | `execution/auto_pm.py`（ bracket、半仓 TP、移保本） |

- `reclaim_watcher.py`：回踩类建议的 reclaim 监视（st_super 跳过）
- `auto_strategy.py`：`AutoRunner` 薄别名
- Dashboard / multi：**Agent执行** 开关（`settings:{sym}.auto_strategy` / `auto_observe`）须与批准决策一致

---

## 八、新增目录（均未提交）

```text
approval/          # 扫描、建单、Redis 建议读写、pending 清理
signals/           # st_super、回踩、指标、touch 检测
execution/         # AutoPM、单元状态机
portfolio/         # RiskGate、配置
feishu/            # 卡片、客户端、notifier
gateway/           # 飞书 ↔ Cursor Agent
nautilus_mcp/      # MCP Server
scripts/           # scan/approve/health/backtest 等 CLI
frontend/public/shared/   # status-bar, alpha-cell, api-auth
docs/              # 本文档及 ALPHA_AGENT, MCP_ALPHA, SIGNAL_SPEC, FEISHU_SETUP
env.example        # 环境变量模板
```

---

## 九、已移除

| 项 | 说明 |
|----|------|
| `openclaw/` 整目录 | OpenClaw / 龙虾 Agent（与交易系统无关，已删） |
| `scripts/setup_openclaw.py` | OpenClaw 初始化 |
| `scripts/bootstrap-lobster.sh` | 弃用入口 |

原 openclaw 内有用脚本已迁至 `scripts/`（如 `push_proposal_feishu.py`）。

---

## 十、启动与验证

```bash
cp env.example .env          # 填写 NAUTILUS_API_SECRET、ALPHA_SYMBOLS 等
bash scripts/start-stack.sh  # Redis + 前端 :3000
python main.py               # 引擎 + SignalDetector + AutoRunner

# 扫一轮超级信号
python scripts/scan_signals.py
# 或 Cursor Chat: alpha

# 健康检查
python scripts/health_check.py
python scripts/test_st_super_signal.py
```

Web：

- 四宫格：http://localhost:3000/multi.html  
- 建议列表：http://localhost:3000/proposals.html  

---

## 十一、已知限制 / 待办

| 项 | 说明 |
|----|------|
| 扫描非引擎内置 cron | 需 MCP / 系统 cron 定期 `scan_signals.py` |
| 四宫格换槽 | 全页 reload（非单格热插拔） |
| `index.html` | 未改超级信号专用 UI（按约定保持原样） |
| 审批 API | 设 `NAUTILUS_API_SECRET` 后前端首次操作需输入密钥 |

---

## 相关文档

| 文档 | 内容 |
|------|------|
| [MCP_ALPHA.md](./MCP_ALPHA.md) | Cursor MCP 配置与日常 |
| [ALPHA_AGENT.md](./ALPHA_AGENT.md) | 架构与常用命令 |
| [SIGNAL_SPEC.md](./SIGNAL_SPEC.md) | 指标与 Redis 约定（回踩规格；st_super 见本文） |
| [FEISHU_SETUP.md](./FEISHU_SETUP.md) | 飞书审批配置 |
| [../env.example](../env.example) | 全部环境变量 |

---

## 变更文件概览（git）

**已跟踪文件的修改**：`main.py`, `strategy.py`, `order_actor.py`, `exit_manager.py`, `events.py`, `frontend/server.js`, `frontend/public/multi.html`, `index.html`, `indicators.html`, `README.md`, …

**新增未跟踪**：`approval/`, `signals/`, `signal_detector.py`, `auto_runner.py`, `execution/`, `portfolio/`, `feishu/`, `gateway/`, `nautilus_mcp/`, `scripts/`, `docs/`, `frontend/public/shared/`, 等。

提交前建议排除：`.engine.pid`, `.frontend.pid`, `.claude/`, 本地 `.env`。
