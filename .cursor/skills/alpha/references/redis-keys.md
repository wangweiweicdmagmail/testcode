# Redis Keys（MCP 只读 / create_proposal）

| Key | 用途 |
|-----|------|
| `bars:1m:{SYMBOL}` | M1 OHLC + vwap |
| `bars:5m:{SYMBOL}` | M5 + st_value/st_dir/dema20 |
| `indicators:active:{SYMBOL}` | M5 冻结 ST + DEMA20 |
| `signals:touch:{SYMBOL}` | 最近触线 JSON list |
| `signals:touch:index` | 触线 ZSET 索引 |
| `proposal:pending:{id}` | 待审批 HASH |
| `proposal:pending:index` | 待审批 ZSET |
| `proposal:dedup:{id}` | 去重 |

MCP 工具（扫描流）：

| 工具 | 用途 |
|------|------|
| `purge_pending_proposals` | 扫描前清理 pending → rejected |
| `run_alpha_scan` | purge + 增量扫描（`purge_before` 默认 true） |
| `audit_m5_st` | M5 K 线 ST 重放 vs Redis / indicators:active |

审批：`POST /api/proposals/:id/decision`（Web server.js），勿 MCP 直接改 approved。
