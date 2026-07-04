# 信号与指标规格（定稿）

> Session：美东 RTH **09:30–16:00 ET**（与 `strategy.py` 一致，盘前/盘后不写 Redis）。

---

## 一、更新频率（三条硬规则）

| 指标 / 行为 | 更新 / 检测频率 | 触发点 |
|-------------|-----------------|--------|
| **SuperTrend** | **每 5 分钟** | M5 K 线收盘 |
| **DEMA(20)** | **每 5 分钟** | M5 K 线收盘 |
| **Session VWAP** | **每 1 分钟** | M1 K 线收盘 |
| **回踩触线检测** | **每 1 分钟** | M1 K 线收盘（不等 M5 收盘） |

```text
M5 收盘  →  更新 ST、DEMA20（写入 active 水平线）
M1 收盘  →  更新 VWAP
M1 收盘  →  用当前水平线做回踩检测 → 可能产生 touch 事件
```

---

## 二、指标定义

### 2.1 SuperTrend（M5）

- **输入**：M5 OHLC 序列（参数与现网一致：period=10, mult=3.0）
- **输出**：`st_value`, `st_dir`（1=多 / -1=空）
- **更新**：每根 **已收盘 M5** 计算一次
- **触线检测用值**：上一档 M5 收盘后的 `st_value`，在 **下一根 M5 收盘前保持不变**

### 2.2 DEMA(20)（M5）

- **输入**：M5 `close` 序列，period=20
- **公式**：`DEMA = 2×EMA(close) − EMA(EMA(close))`
- **更新**：每根 **已收盘 M5** 计算一次
- **触线检测用值**：同上，M5 冻结至下一根 M5 收盘

### 2.3 Session VWAP（与周期无关，按 M1 刷新）

- **起点**：当日 RTH **09:30 ET** 第一根 M1 起累计（新交易日 reset）
- **公式**：

  ```text
  typical_price = (high + low + close) / 3
  VWAP = Σ(typical_price × volume) / Σ(volume)
  ```

- **更新**：每根 **已收盘 M1** 增量累计一次（市场常用做法）
- **说明**：VWAP 定义不绑定 M1/M5；选 M1 更新是为了数值及时、便于 M1 触线检测

---

## 三、Redis 存储

### 3.1 K 线（已有，扩展字段）

```text
bars:1m:{SYMBOL}   每根 JSON 增加:  vwap
bars:5m:{SYMBOL}   已有:  st_value, st_dir, ema21, … ；Plan 增加:  dema20
```

### 3.2 当前生效水平线（M5 冻结，供 M1 检测）

```text
indicators:active:{SYMBOL}
```

```json
{
  "m5_bar_time": 1718654400,
  "supertrend": { "value": 481.90, "dir": 1 },
  "dema20": 481.50,
  "updated_at": 1718654405
}
```

- **写入时机**：每 M5 收盘
- **读取方**：M1 触线检测、MCP、Agent

### 3.3 触线事件（M1 检测产出，非 proposal）

```text
signals:touch:{SYMBOL}          最新一条触线快照（可选）
signals:touch:index             ZSET，score=touch_time
PUBLISH signal:touch            可选，推前端 / 唤醒 Agent
```

```json
{
  "symbol": "NVDA",
  "signal_type": "pullback_supertrend",
  "side": "LONG",
  "trigger_level": 481.90,
  "level_source": "indicators:active.supertrend",
  "touch_time": 1718654520,
  "m1_bar_time": 1718654520,
  "m5_context_bar_time": 1718654400,
  "m1_high": 482.0,
  "m1_low": 481.85,
  "m1_close": 481.95,
  "reclaim": true
}
```

`signal_type` 枚举：

- `pullback_vwap`
- `pullback_supertrend`
- `pullback_dema20`

---

## 四、M1 回踩检测规则

**共性**：在每根 **M1 收盘** 执行；触线 = `m1.low <= level <= m1.high`。

| 策略 | 水平线来源 | 水平线更新 | 顺势 | Reclaim（默认） |
|------|------------|------------|------|-----------------|
| VWAP | 本 M1 收盘后的 `vwap`（或检测用上一根 M1 的 vwap，实现时二选一并写死） | M1 | **M5** `st_dir` 与方向一致 | 前一根 M1 收盘在 VWAP 一侧，本根触线且 close reclaim |
| SuperTrend | `indicators:active.supertrend` | M5 冻结 | **M5** `st_dir==1` 多 / `-1` 空 | 触线且 `close >= st`（多）或 `close <= st`（空） |
| DEMA20 | `indicators:active.dema20` | M5 冻结 | **M5 ST 方向**（同左） | 同 ST |

**去重**：同一 `symbol + signal_type + m5_context_bar_time`（VWAP 用 `session_date + signal_type`）只触发 **一次**。

---

## 五、时间线示例

```text
09:30  M1#1 收盘 → vwap=v0
09:31  M1#2 收盘 → vwap=v1；检测 VWAP/ST/DEMA 触线（ST/DEMA 尚无 → 跳过或等首根 M5）
09:35  M5#1 收盘 → 更新 active.st, active.dema20
09:36  M1 触 active.st → 立即写 signals:touch（不等 09:40 M5）
09:36  vwap 仍按 M1 更新
09:40  M5#2 收盘 → 刷新 active.st, active.dema20
```

---

## 六、模块职责

| 模块 | 职责 | 频率 |
|------|------|------|
| `strategy.py` | M1/M5 指标计算；写 `bars:*`、`vwap`、`indicators:active` | M1 / M5 收盘 |
| `signal_detector`（Plan 新增，可先在 strategy M1 末尾调用） | M1 触线检测；写 `signals:touch` | M1 收盘 |
| MCP Server | 只读指标 / touch；`create_proposal` | 按需 |
| Agent Scheduler | 读 touch + 大盘 → 1 条 proposal | RTH 每 5 分钟 |

**禁止**：在 M5 收盘时才做触线检测（会漏掉 M5 内部的 alpha）。

---

## 七、与 Agent 的关系

- **客观事实**：M1 触线 → `signals:touch`（Python 算好）
- **主观决策**：Agent 每 5 分钟读 touch + QQQ/全市场 → 选方向 + 最佳策略 → `create_proposal`
- **执行**：人审批 → `auto_runner`

---

## 八、实现顺序（Plan）

1. `strategy.py`：M1 增量 VWAP + M5 DEMA20 + `indicators:active`
2. M1 `signal_detector`：三策略触线 + 去重 + Redis
3. MCP 只读工具对齐本 spec
4. Agent Scheduler + prompt
