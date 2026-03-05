# 鹦鹉螺引擎 IBKR — 项目上下文

## ⚠️ 时区（必读，禁止再犯）

| 时区 | UTC 偏移 | 备注 |
|------|---------|------|
| 上海 / 北京（CST） | UTC+8 | 系统显示的本地时间 |
| 纽约（ET 夏令时 EDT） | UTC-4 | 美股交易时段 3-11月 |
| 纽约（ET 冬令时 EST） | UTC-5 | 美股交易时段 11-3月 |

**换算公式（夏令时）：**
- 北京时间 → 纽约时间：`北京时间 - 12小时`
- 例：北京 23:20 = 纽约 11:20（完全在交易时段内）

**美股交易时段（纽约时间）：**
- 盘前：04:00 – 09:29
- 正市：09:30 – 16:00
- 盘后：16:00 – 20:00

**北京时间对应：**
- 夏令时正市：北京 21:30 – 次日 04:00
- 冬令时正市：北京 22:30 – 次日 05:00

> [!CAUTION]
> AI 助手看到的系统时间是北京时间。凡是涉及美股是否在交易的判断，必须先换算为纽约时间再做结论，禁止直接用北京时间判断。

---

## 项目概述

- **项目名称**：鹦鹉螺引擎（Nautilus Trader × IBKR）
- **目的**：连接 Interactive Brokers 进行美股实盘量化交易
- **架构**：三层（引擎 Python → Redis → 前端 Node.js）

## 核心组件

| 文件 | 职责 |
|------|------|
| `main.py` | 入口，配置并启动 TradingNode |
| `strategy.py` | 核心策略：K线聚合、指标计算（SuperTrend/EMA）、写 Redis |
| `order_actor.py` | HTTP 下单网关（端口 8888），订单生命周期管理 |
| `exit_manager.py` | 独立止盈管理器，监听 bar.collected 事件 |
| `events.py` | 自定义 MessageBus 事件类 |
| `frontend/server.js` | Node.js WebSocket 服务器，展示 K 线和指标 |

## 账户配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `IB_ACCOUNT_ID` | FA 主账号 | `F10251881` |
| `IB_FA_GROUP` | FA Group 名称 | `dt_test` |
| `IB_FA_METHOD` | 分配方法 | `NetLiq` |
| `IBG_PORT` | TWS/Gateway 端口 | `7496`（实盘 TWS） |
| `IBG_CLIENT_ID` | API 客户端 ID | `2` |

## 启动命令

```bash
# 实盘
cd /Users/weiweiwang/testcode/nautilus_ibkr_helloworld
python main.py

# 回测
python main.py --mode backtest

# 前端
cd frontend && node server.js
```

> [!IMPORTANT]
> **重启规则**：说"重启"默认表示**前端 + 引擎同时重启**。只有明确说"重启前端"时才只重启前端。
>
> 重启命令：
> ```bash
> # 重启引擎（前后端都重启时）
> pkill -f "python main.py"; pkill -f "node server.js"
> cd /Users/weiweiwang/testcode/nautilus_ibkr_helloworld && python main.py > /tmp/engine.log 2>&1 &
> cd frontend && node server.js > /tmp/frontend.log 2>&1 &
> ```


## HTTP 端点

### 引擎端（order_actor.py 端口 8888）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/account` | 账户余额（FA Group 聚合） |
| GET | `/positions` | 当前开放仓位 |
| GET | `/active-orders` | 活跃止损单 + 入场价（供前端恢复价格线） |
| GET | `/debug-orders` | 调试：打印 cache 中所有订单和持仓 |
| POST | `/order` | 下单（MARKET / LIMIT / BRACKET） |
| POST | `/settings` | 策略开关（st_trail 跟踪止盈） |
| POST | `/close` | ⏳ 待实现：反向市价平仓 + 取消止损单 |
| POST | `/modify-stop` | 修改活跃止损单触发价（body: {symbol, price}） |

### Node.js 端（server.js 端口 3000）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/data/:symbol` | 历史 K 线 + 仓位 + 昨日围栏 |
| GET | `/api/indicators` | 所有标的最新指标 |
| GET | `/api/account` | 账户余额（优先 Redis，fallback 引擎） |
| GET | `/api/positions` | 仓位（代理引擎） |
| GET | `/api/active-orders` | 活跃订单（代理引擎） |
| POST | `/api/order/:symbol` | 下单（代理引擎） |
| DELETE | `/api/position/:symbol` | 平仓 ⚠️ 目前仅删 Redis，引擎平仓待实现 |
| POST | `/api/modify-stop/:symbol` | 修改止损价（代理引擎 + 更新 Redis position.stop_loss） |
| POST | `/api/settings/:symbol` | 策略开关，同步到引擎 |

## 已实现功能汇总

- **盘前盘后数据过滤** ✅：引擎层（`strategy.py`）和前端 API（`server.js`）均过滤非正市 RTH 数据（仅保留 09:30–16:00 ET）；盘后 bar/tick 不进指标计算、不写 Redis、不推送前端
- **M1/M5 ST 参数分离** ✅：M1 ST 用（period=10, mult=3.5），M5 ST 用（period=10, mult=3.0）；`BarLoggerStrategyConfig` 新增 `st_mult_m5` 字段；引擎日志确认显示 `M1-ST(10,3.5)  M5-ST(10,3.0)`
- **trail_mode 重构（三套互斥止盈）** ✅：`settings:{sym}.trail_mode` 单字段控制（0=关, 1=M1-ST, 2=M5-ST, 3=EMA-M5）；`ExitManager` 每次 bar 收盘直接从 Redis 读开关，无内存状态；前端三个 toggle 互斥
- **M5 bar 收盘事件** ✅：`events.py` 新增 `BarCollectedM5Event`；`strategy.py` 在 `_process_m5_bar` 发布 `bar.collected.m5`；`ExitManager` 订阅后处理 M5-ST 跟踪（每5分钟一次）
- **mom_atr 归一化动量指标** ✅：新增 `_MomentumATRState` 状态机；公式 = `(close_now - close_2bars_ago) / ATR_14`（15分钟窗口 ÷ Wilder ATR-14）；写入 `bars:5m:{sym}` 的 `mom_atr` 字段，ATR 预热前为 `None`
- **平仓按钮** ✅：前端 → `DELETE /api/position/:symbol` → `POST /close`（引擎）→ 取消止损单 + 市价平仓；引擎离线时降级为仅删 Redis 记录
- **持仓止损价修改** ✅：点击图表止损价格线 → 橙色药丸 → 拖动 → 松手调用 `POST /api/modify-stop/:symbol` → 引擎 `modify_order()` 修改 IBKR 止损单触发价；止损成交/平仓后价格线自动清除
- **止损单 ID 持久化** ✅：止损单 ACCEPTED 后将 `client_order_id` 写入 Redis `order:stop:{sym}`；引擎重启后 IBKR 重新推送 ACCEPTED 事件，cache 回充，`modify_order()` 即可正常运作
- **全标的指标排行** ✅：`indicators.html` 从 `/api/indicators` 拉取全部 13 个标的的 M1 ST 积分 / M5 ST 积分 / EMA 偏离 / 日内新高，四列并排排行
- **EMA21 偏差修复** ✅：修复 `_M5Bucket.flush_current()` 不清空 `_bars` 导致 EMA 二次喂入的 Bug，以及 `_flush_history_for()` 对未完成 M5 bar 不再调用 `update()`

## Redis 数据结构

| Key | 类型 | 内容 |
|-----|------|------|
| `bars:1m:{sym}` | List | 1分钟 K 线历史（仅正市 RTH） |
| `bars:5m:{sym}` | List | 5分钟 K 线历史（含 `mom_atr` 指标，仅正市） |
| `position:{sym}` | String | 仓位 JSON（含 `stop_loss`） |
| `settings:{sym}` | String | 策略开关 JSON：`{"trail_mode": 0/1/2/3}` |
| `order:stop:{sym}` | String | 活跃止损单 `{client_order_id, trigger_price}` — 重启恢复用 |
| `kline:1m:{sym}` | PubSub | 实时 1m K 线推送（仅 RTH） |
| `kline:5m:{sym}` | PubSub | 实时 5m K 线推送（含 `mom_atr`） |
| `order:update` | PubSub | 订单状态变更推送 |
| `account:funds` | String | 账户余额 JSON |
| `engine:heartbeat` | PubSub | 引擎心跳（每 5s） |

## 指标字段说明

| 字段 | 所在 bar | 说明 |
|------|---------|------|
| `st_value` | M1/M5 | SuperTrend 值（M1:10,3.5 / M5:10,3.0） |
| `st_dir` | M1/M5 | ST 方向：`1`=多头, `-1`=空头 |
| `ema21` | M1/M5 | EMA21 |
| `mom_atr` | M5 | 归一化15分钟动量 = `(C - C_2bars_ago) / ATR_14`（预热14根后有值） |
