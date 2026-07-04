# Alpha 信号 Agent

> **当前方案**：MCP `alpha` / `scripts/scan_signals.py` + Redis 审批 + `auto_runner` 执行。

---

## 架构

```text
main.py (引擎) → SignalDetector → signals:touch
       ↓
run_alpha_scan (MCP / scripts) → proposal:pending
       ↓
Web / 飞书 人工审批
       ↓
auto_runner (Dashboard「Agent执行」) → IBKR
```

---

## 启动（日常）

```bash
cd /path/to/nautilus_ibkr_helloworld
source scripts/env.sh

bash scripts/start-stack.sh    # Redis + 前端 + 可选 feishu notifier
python main.py                 # 需 TWS/Gateway
```

停止：

```bash
bash scripts/stop-stack.sh
```

---

## 环境变量

复制 `env.example` → `.env`。关键项：

| 变量 | 说明 |
|------|------|
| `ALPHA_SYMBOLS` | 扫描标的 |
| `ALPHA_PRIMARY_SIGNAL` | 默认 `st_super` |
| `FEISHU_*` | 可选，手机审批卡片 |
| `NAUTILUS_API_SECRET` | 前端审批/下单 API 密钥 |

---

## 常用命令

```bash
python scripts/scan_signals.py
python scripts/list_proposals.py --status pending
python scripts/approve_proposal.py --id <id> --decision approved_live
python scripts/health_check.py
```

MCP：见 [MCP_ALPHA.md](./MCP_ALPHA.md)

---

## 审批

- Web：`http://localhost:3000/proposals.html`、四宫格 `multi.html`
- 飞书：见 [FEISHU_SETUP.md](./FEISHU_SETUP.md)

批准后须在 Dashboard 将 **Agent执行** 设为 **观察** 或 **实盘**，`auto_runner` 才会下单。

未提交功能总览见 [UNRELEASED.md](./UNRELEASED.md)。
