# Alpha Agent：Cursor MCP 方案

> **推荐**：Cursor 客户端 + MCP `nautilus-alpha` + Skill `nautilus-alpha`  
> **已弃用（可选）**：`alpha_agent.py` 独立进程 + 云 LLM（DeepSeek）

---

## 架构

```text
main.py → Redis (bars / indicators:active / signals:touch)
              ↓
Cursor Agent（本机模型）
  ← MCP alpha（只读 + create_proposal）
  ← Skill .cursor/skills/alpha
              ↓
proposal:pending → Web / 飞书审批 → ReclaimWatcher → AutoRunner
```

---

## 一次性配置

```bash
cd /Users/weiweiwang/testcode/nautilus_ibkr_helloworld
bash scripts/setup-mcp.sh    # 创建 .venv + 安装 mcp/redis

# Cursor：Settings → MCP → 应自动加载 .cursor/mcp.json（command: mcp/run.sh）
```

验证 MCP（Cursor 对话）：

```text
用 MCP alpha 调用 get_stack_health
```

---

## 日常启动

```bash
bash scripts/start-stack.sh   # 前端 + 飞书；不再默认启动 alpha_agent.py
python main.py                # 引擎
```

手动跑一轮（Cursor Chat 或 CLI）：

```text
加载 skill alpha，执行 MCP run_alpha_scan
```

或 CLI：

```bash
python scripts/scan_signals.py          # 增量（cron 同逻辑）
python scripts/scan_signals.py --full   # 调试：回放 24h 触线
```

未提交功能详见 [UNRELEASED.md](./UNRELEASED.md)。

---

## Cursor Automations（定时）

1. Automations → New → **Cron** 每 1 分钟（RTH 时段自行换算）
2. Tools → **MCP** → 选择 **alpha**
3. Prompt 使用 Skill 内模板（见 `.cursor/skills/nautilus-alpha/SKILL.md`）

---

## 环境变量

| 变量 | 说明 |
|------|------|
| `REDIS_HOST` / `REDIS_PORT` | MCP 连 Redis |
| `ALPHA_SYMBOLS` | 默认扫描标的 |
| `ALPHA_PROPOSAL_TTL_SECONDS` | proposal 过期（默认 30min） |

`DEEPSEEK_*` 仅 **legacy** `alpha_agent.py` 需要；MCP 方案不需要。

---

## Legacy：alpha_agent.py

仍可用作无 Cursor 时的后备：

```bash
python alpha_agent.py
```

建议设置 `ALPHA_USE_DAEMON=0` 在 `start-stack.sh` 中保持关闭。

---

## MCP 工具列表

见 `nautilus_mcp/server.py`：`run_alpha_scan`（cron 推荐）、`get_alpha_snapshot`、`create_proposal` 等。

规格对齐：`docs/SIGNAL_SPEC.md`
