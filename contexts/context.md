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

- **平仓按鈕** ✅：前端 → `DELETE /api/position/:symbol` → `POST /close`（引擎）→ 取消止损单 + 市价平仓；引擎离线时降级为仅删 Redis 记录
- **持仓止损价修改** ✅：点击图表止损价格线 → 橙色药丸出现 → 拖动 → 松手调用 `POST /api/modify-stop/:symbol` → 引擎 `modify_order()` 修改 IBKR 止损单触发价；成功后原价格线移动到新价格，止损成交/平仓后价格线自动清除
- **ST 跟踪止损** ✅：开仓后开启“ST跟踪止盈”开关 → `ExitManager` 每分钟 K 线收盘后自动计算新止损价（檘轮机制：多头只週上移，空头只週下移）并调用 `modify_order()` 修改 IBKR 止损单
- **止损单 ID 持久化** ✅：止损单 ACCEPTED 后将 `client_order_id` 写入 Redis `order:stop:{sym}`；引擎重启后 IBKR 重新推送 ACCEPTED 事件，cache 回充，`modify_order()` 即可正常运作
- **全标的指标排行** ✅：`indicators.html` 从 `/api/indicators` 拉取全部 13 个标的（NVDA/AAPL/GOOG/AVGO/SPY/TSLA/PLTR/AMZN/AMD/META/MSFT/QQQ/TSM）的 M1 ST 积分 / M5 ST 积分 / EMA 偏离 / 日内新高，四列并排对比排行；`server.js` 的 `ALL_SYMBOLS` 和 `SYMBOL_MAP` 已同步扩展到 13 个标的

## Redis 数据结构

| Key | 类型 | 内容 |
|-----|------|------|
| `bars:1m:{sym}` | List | 1分钟 K 线历史 |
| `bars:5m:{sym}` | List | 5分钟 K 线历史 |
| `position:{sym}` | String | 仓位 JSON（含 `stop_loss`） |
| `settings:{sym}` | String | 策略开关 JSON（含 `st_trail`） |
| `order:stop:{sym}` | String | 活跃止损单 `{client_order_id, trigger_price}` — 重启恢复用 |
| `kline:1m:{sym}` | PubSub | 实时 1m K 线推送 |
| `kline:5m:{sym}` | PubSub | 实时 5m K 线推送 |
| `order:update` | PubSub | 订单状态变更推送 |
| `account:funds` | String | 账户余额 JSON |
| `engine:heartbeat` | PubSub | 引擎心跳（每 5s） |
