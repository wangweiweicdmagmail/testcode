# 鹦鹉螺引擎 (NautilusTrader) × IBKR 实盘交易系统

基于 **NautilusTrader** 框架连接 **Interactive Brokers (IBKR)**，支持多标的实时 K 线、SuperTrend 指标计算、HTTP 下单网关和前端可视化 Dashboard。

## 项目结构

```
nautilus_ibkr_helloworld/
├── main.py            # 主程序：配置并启动 TradingNode（支持 --mode live/backtest）
├── strategy.py        # 回测/实盘通用策略：1m K 线 + SuperTrend + EMA + 写Redis
├── order_actor.py     # HTTP 下单网关 Actor（端口 8888）
├── order_sender.py    # 外部下单测试脚本（MARKET / BRACKET）
└── frontend/
    ├── server.js      # Node.js WebSocket 服务器，从 Redis 推送 K 线给前端
    └── public/
        ├── index.html # 单图 Dashboard（Lightweight Charts + 止损拖动开仓）
        └── multi.html # 四图总览（2×2 网格，每图独立止损拖动开仓）
```

## 依赖

### Python
```bash
pip install uv
uv pip install "nautilus_trader[ib]" redis
```

### Node.js（前端）
```bash
cd frontend && npm install
```

### Redis
```bash
# macOS
brew install redis && brew services start redis

# 或 Docker
docker run -d -p 6379:6379 redis
```

## 环境准备

### 1. 启动 TWS 或 IB Gateway

- 登录 [TWS](https://www.interactivebrokers.com/en/trading/tws.php) 或 IB Gateway
- 开启 API 连接：`Edit → Global Configuration → API → Settings → Enable ActiveX and Socket Clients`

| 类型       | 端口（实盘） | 端口（模拟） |
|------------|------------|------------|
| TWS        | `7496`     | `7497`     |
| IB Gateway | `4001`     | `4002`     |

### 2. 修改配置

编辑 `main.py` 中的用户配置区：

```python
IBG_PORT    = 7496              # TWS/Gateway 端口
ACCOUNT_ID  = "F10251881"       # FA 主账号 ID
FA_GROUP    = "dt_test"         # FA Group 名称（留空则不使用 FA 分配）
FA_METHOD   = "NetLiq"          # FA 分配方式
```

或通过环境变量传入：

```bash
export IB_ACCOUNT_ID="F10251881"
export IB_FA_GROUP="dt_test"
```

## 启动顺序

```bash
# Step 1：启动交易引擎
# 实盘模式：连接 IBKR，拉取今日历史 K 线，启动实时订阅
python main.py

# 回测模式：连接 IBKR，拉取上一交易日 K 线，写入 Redis 后不订阅实时
python main.py --mode backtest

# 回测指定日期（IBKR 支持范围表1年内的 1m 数据）
python main.py --mode backtest --date 2026-02-25

# Step 2：启动前端 WebSocket 服务器
cd frontend && node server.js

# 打开浏览器：http://localhost:3000
# 积分排行：http://localhost:3000/indicators.html
# 四图总览：http://localhost:3000/multi.html
```

> **模式对毕**
> | 项目 | 实盘（live） | 回测（backtest） |
> |------|------|------|
> | IBKR 连接 | ✅ 必须 | ✅ 必须 |
> | 数据来源 | 今日 IBKR 历史 K 线 | IBKR 上一交易日数据 |
> | 实时 Bar 订阅 | ✅ | ❌（不订阅） |
> | Tick 订阅 | ✅ | ❌（不订阅） |
> | Redis 写入 | ✅ | ✅ |
> | 前端可用 | ✅ | ✅ |

## 前端 UI 功能

### 止损拖动开仓（单图 & 四图）

点击面板右上角 **"开仓"** 按钮后，进入交互式开仓模式：

1. **止损药丸控件**出现在图表右侧（红色圆角标签，带脉冲动画）
2. **上下拖动**控件来调整止损价，药丸实时跟随并显示最新止损价
3. 松手后止损线与控件精确对齐
4. **订单面板**同步显示计算结果：

| 字段 | 说明 |
|------|------|
| 最大亏损 ($) | 用户输入，默认 $100 |
| 止损价 | 仅由拖动控件设置（只读显示） |
| 方向 | **自动判断**：止损 < 当前价 → ▲ 做多；止损 > 当前价 → ▼ 做空 |
| 建议股数 | `floor(最大亏损 / 每股风险)`，只读自动计算 |
| 每股风险 | `|当前价 - 止损价|` |
| 止损幅度 | `每股风险 / 当前价 × 100%` |
| 开仓金额 | `建议股数 × 当前价` |
| 资金占比 | 按账户 $10 万计算占比 |

5. 点击 **确认做多/做空** 发送开仓请求；点击 **取消** 退出模式


## 发送测试订单

```bash
# 普通市价单
python order_sender.py

# 括号单（市价入场 + 止损 + 定时止损移动）
python order_sender.py --bracket
```

## 支持的订单类型

| 类型      | 说明 |
|-----------|------|
| `MARKET`  | 市价单（DAY，立即成交） |
| `LIMIT`   | 限价单（GTC） |
| `BRACKET` | 括号单：市价入场 + 止损单（OCA 联动），支持定时移动止损 |

HTTP API 示例：

```bash
# 市价单
curl -X POST http://localhost:8888/order \
  -H "Content-Type: application/json" \
  -d '{"instrument_id":"QQQ.NASDAQ","side":"BUY","qty":1,"order_type":"MARKET"}'

# 括号单（止损 600，60s 后移至 602，再 60s 后移至 603）
curl -X POST http://localhost:8888/order \
  -H "Content-Type: application/json" \
  -d '{"instrument_id":"QQQ.NASDAQ","side":"BUY","qty":1,"order_type":"BRACKET",
       "stop_loss":600,"sl_steps":[602,603],"sl_step_secs":60}'
```

## Redis Key 约定

| Key                      | 用途 |
|--------------------------|------|
| `bars:1m:{SYMBOL}`       | 1m K 线列表（含 SuperTrend、EMA21） |
| `bars:5m:{SYMBOL}`       | 5m 聚合 K 线列表 |
| `position:{SYMBOL}`      | 仓位信息 |
| `settings:{SYMBOL}`      | 策略开关（如 ST 跟踪止盈） |
| `kline:1m:{SYMBOL}`      | PUBLISH：K 线收盘事件 |
| `kline:1m:tick:{SYMBOL}` | PUBLISH：Tick 实时更新 |

## ⚠️ 项目核心要求

> **全部数据来自 IBKR，无外部数据源依赖。** 无论回测还是实盘，均通过 IBKR `request_bars()` 获取真实 K 线。

- 实盘模式：拉取今日盘前数据（04:00 ET 起）+ 订阅实时；图表仅显示 RTH（09:30-16:00 ET）
- 回测模式：拉取上一交易日（或指定日期）数据，写入 Redis 后不订阅实时
- **盘前数据**：自动用于指标（SuperTrend/EMA）预热，但**不写入图表**
- **盘后数据**：完全忽略（不处理）

## 指标参数

| 指标 | 参数 |
|------|------|
| SuperTrend | period=10, multiplier=**2.0** |
| EMA | period=21 |

## FA Group 配置说明

本项目通过 Monkey-Patch 扩展了 `IBOrderTags`，支持 IBKR FA（Financial Advisor）账号的 Group 分配：

```python
# main.py 中配置
FA_GROUP  = "dt_test"   # FA Group 名称
FA_METHOD = "NetLiq"    # 分配方式
```

留空 `FA_GROUP` 则直接在单账号下单，不通过 FA 分配。

## 支持的合约格式（IB_SIMPLIFIED 模式）

- 美股：`AAPL.NASDAQ` / `QQQ.NASDAQ` / `SPY.ARCA` / `TSLA.NASDAQ`
- 期货：`ESH5.CME` / `NQH5.CME`
- 外汇：`EUR/USD.IDEALPRO`
- 指数：`^SPX.CBOE`

## 常见问题

| 问题 | 解决方案 |
|------|--------|
| `Connection refused` | 检查 TWS/Gateway 是否已启动，API 是否已开启 |
| `Could not find instrument` | 确认 `INSTRUMENT_ID` 格式正确，格式为 `代码.交易所` |
| 无实时数据 | 将 `MARKET_DATA_TYPE` 改为 `IBMarketDataTypeEnum.DELAYED_FROZEN` |
| 时区异常 | NautilusTrader 内部全部使用 UTC 纳秒时间戳；K 线前端展示使用 ET fake-UTC |
| 图表只显示 390 根 | 正常。盘前（570 根）用于指标预热，图表仅展示 RTH 09:30-16:00 的 390 根 |
| ST 预热不准 | 正常。盘前 570 根历史数据已用于预热，RTH 第一根的 ST 值是经过充分预热的 |
| 前端提示「引擎未连接」 | 重启 `server.js`（旧进程可能缺少 `/api/account` 路由）|

## 订单生命周期日志

`OrderGatewayActor` 已实现完整的 NautilusTrader 订单回调，实盘日志覆盖如下：

| 日志前缀 | 时机 | 级别 |
|----------|------|------|
| `[HTTP] ← POST /order` | HTTP 请求到达（跨线程 print）| INFO |
| `[Order] ⬇ 收到下单指令` | MessageBus 分发给 Actor | INFO |
| `[Order] → submit MARKET/LIMIT` | submit_order 调用前 | INFO |
| `[Gateway] ✅ 订单已提交` | submit_order 返回后 | INFO |
| `[Order] ✅ ACCEPTED` | 交易所接受，附 VenueOrderId | INFO |
| `[Order] ❌ REJECTED` | 交易所拒绝，附拒绝原因 | ERROR |
| `[Order] ❌ DENIED` | NautilusTrader 风控拒绝 | ERROR |
| `[Order] ✅ FILLED` | 完全成交，附成交价/量/佣金 | INFO |
| `[Order] 🔶 PARTIALLY_FILLED` | 部分成交，附本次/累计/剩余 | INFO |
| `[Order] 🔔 TRIGGERED` | 止损单触发（stop price 到达）| WARNING |
| `[Order] ⛔ CANCELED` | 订单取消 | INFO |
| `[Order] ⌛ EXPIRED` | DAY 单收市未成交 | WARNING |
| `[Gateway] 止损修改 Step N` | 止损价定时移动 | INFO |

## 真实 IBKR 仓位对接

前端仓位面板直接使用 IB 真实持仓数据，数据流：

```
成交 → strategy.py on_position_changed()
   → redis.publish("position:update", {avg_px_open, unrealized_pnl, side, quantity, ...})
   → server.js psubscribe("position:*")
   → WebSocket 广播 channel="position:update"
   → index.html / multi.html 实时刷新仓位面板
```

- 仓位面板字段适配：`avg_px_open`（均价）、`unrealized_pnl`（浮盈）、`side`（多/空）
- 每根 K 线收盘时自动重算浮盈（含做空方向）
- 平仓时推送 `{closed: true}` 自动清空面板
- 旧 Redis 格式（`entry_price / pnl / pnl_pct`）依然兼容，无需迁移

