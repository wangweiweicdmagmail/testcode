---
name: alpha
description: >-
  M1 pullback alpha scan via MCP alpha. Triggers: alpha, 扫信号, 跑一轮.
  Agent MUST list all branches, skip reasons, and alternatives in every reply.
  Use run_alpha_scan (purge_before=true) for cron; see references/debug-playbook.md.
---

# Alpha 信号扫描

## 触发

`alpha` / `扫信号` / `跑一轮` / `扫一眼` / Automations cron（MCP `alpha`）

---

## Agent 回复模板（每次必须按此结构输出）

用户发 `alpha` 后，**不要只给结论**，须包含可调试的完整过程：

```markdown
## Alpha 扫描

### 1. 前置
- Redis: ✓/✗
- 引擎 heartbeat: 有/无
- 扫描标的: NVDA,TSLA,AAPL

### 2. 清理（purge_before）
- purged: N 条（无则写 0）
- 若有: 列表 symbol side reason（最多 5 条 + 「另有 M 条」）

### 3. 扫描结果
- **结论**: NO_OP / 新建 N 条
- **created**（若有）: symbol | side | signal_type | rr_half_est | trigger | proposal_id
- **skipped**（若有）: 逐条 symbol side type reason（中文释义见下表）

### 4. 本轮未走的路径（备选说明）
- 未调用 create_proposal（增量扫描已自动建单 / 无触线）
- 未用 --full 回放
- 审批入口: /proposals.html

### 5. 下一步（仅当有 pending）
- 打开审批页；批准后为 conditional_reclaim，等 reclaim 才下单
```

`NO_OP` 时仍须写出 **skipped 摘要**（为何有触线但未建单）和 **health**。

---

## 标准流程（推荐）

```
run_alpha_scan(incremental=true, purge_before=true)
  ├─ purge_pending_proposals
  └─ run_incremental_scan
```

| 步骤 | MCP 工具 | 何时改用备选 |
|------|----------|--------------|
| 1 健康 | `get_stack_health` | heartbeat 无 → 提示 `python main.py` |
| 2 清理+扫描 | `run_alpha_scan` | MCP 不可用 → `scripts/scan_signals.py` |
| 3 深查 | `get_alpha_snapshot` | 需逐标的触线/active 时 |
| 4 单条补建 | `create_proposal` | 仅当 scan 漏掉且用户明确要求 |
| 5 ST 对账 | `audit_m5_st` | counter_trend 异常或用户质疑 ST |

---

## MCP 工具全集（alpha 服务器）

| 工具 | 作用 | 典型参数 |
|------|------|----------|
| `run_alpha_scan` | **主入口** purge + 增量扫描 | `purge_before=true` |
| `purge_pending_proposals` | 仅清理 pending→rejected | `dry_run` 先预览 |
| `get_alpha_snapshot` | 聚合：active + touches + pending | `symbols` 可选 |
| `get_stack_health` | Redis + pending 数 + heartbeat | — |
| `list_recent_touches` | 触线事件 | `symbol`, `limit` |
| `list_pending_proposals` | 待审批列表 | `symbol`, `limit` |
| `get_indicators_active` | M5 冻结 ST/DEMA | `symbol` |
| `get_m1_bars` / `get_m5_bars` | K 线 + 指标字段 | `limit`≤120 |
| `create_proposal` | 手动单条建 pending | touch_time/side/type |
| `audit_m5_st` | ST 重放对账 | `symbol`, `limit` |

---

## 过滤与跳过（须向用户翻译）

### 扫描 skipped.reason

| reason | 中文 |
|--------|------|
| `touch_too_old` | 触线超过 300s（默认） |
| `counter_trend` | 与 M5 ST 逆势 |
| `pending_same_side` | 同标的同向已有待审批 |
| `duplicate_proposal` | 该触线已建过建议 |
| `pricing_failed` | 止损/结构定价失败 |
| `rr_below_min` | 半仓 R:R < 1 |
| `symbol_cap` | 本轮该标的已满 1 条 |
| `scan_cap` | 本轮全市场已满 3 条 |

### purge purged.reason

| reason | 中文 |
|--------|------|
| `expired` | 建议已过期 |
| `touch_stale` | 触线过旧 |
| `counter_trend` | 相对当前 M5 ST 逆势 |

---

## 分支决策（Agent 自检）

```
engine_heartbeat 无?
  → 仍可读 Redis 扫触线，但注明「无实时触线」

purge.purged_count > 0?
  → 列出原因分布 (expired / touch_stale / counter_trend)

scan.no_op && skipped 为空?
  → 说明「无近期触线」；可 get_alpha_snapshot 展示 active

scan.no_op && skipped 非空?
  → **必须**列出 skipped，解释为何未建单

created_count > 0?
  → 列摘要，**不要**再 create_proposal 重复建

用户问「为什么没 NVDA」?
  → list_recent_touches(NVDA) + get_indicators_active(NVDA) + 对照 skip 表

ST/VWAP 可疑?
  → audit_m5_st(symbol) → 建议重启引擎
```

---

## 备选路径（须在一次回复中提及可用项）

| 路径 | 命令/工具 | 限制 |
|------|-----------|------|
| **A 推荐** | MCP `run_alpha_scan` | cron / 对话 |
| B MCP 只读 | `get_alpha_snapshot` | 不建单 |
| C CLI 增量 | `python scripts/scan_signals.py` | 同 A |
| D CLI 全量 | `scan_signals.py --full` | 仅人工调试 |
| E 仅清理 | `purge_pending_proposals(dry_run=true)` | 预览 |
| F 单条补建 | `create_proposal` | 勿重复已扫描触线 |
| G Legacy | `python alpha_agent.py` | 需 DEEPSEEK，默认关闭 |
| H 审批 | Web `/proposals.html` 或 `approve_proposal.py` | 非 MCP 写 approved |

**禁止**：MCP 改 approved、写 settings、绕过审批下单、cron 用 `--full`。

---

## 策略要点

- 顺势：**仅 M5 ST(10,3.0)**，`indicators:active.supertrend.dir`
- 触线：M1 碰 VWAP / M5 ST / M5 DEMA20（`signals:touch`）
- 执行：`conditional_reclaim` → 审批 → reclaim → AutoRunner
- 默认标的：`ALPHA_SYMBOLS=NVDA,TSLA,AAPL`

---

## Automations Prompt

```text
调用 MCP alpha 的 run_alpha_scan（incremental=true, purge_before=true）。
按 Skill 回复模板输出：前置 / purge / created / skipped（含中文 reason）/ 备选路径。
若 no_op 也必须解释 skipped 或「无触线」。
不要对已有触线重复 create_proposal。
```

---

## 环境变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `ALPHA_TOUCH_MAX_AGE_SECONDS` | 300 | 触线时效 / purge |
| `ALPHA_MIN_RR_HALF` | 1.0 | 最低半仓 R:R |
| `ALPHA_MAX_PER_SYMBOL` | 1 | 每轮每标的 |
| `ALPHA_MAX_PER_SCAN` | 3 | 每轮全市场 |
| `ALPHA_SYMBOLS` | NVDA,TSLA,AAPL | 扫描池 |

---

## 延伸阅读

- [references/redis-keys.md](references/redis-keys.md) — Redis key
- [references/debug-playbook.md](references/debug-playbook.md) — 过滤链、CLI、ST/VWAP 排查
