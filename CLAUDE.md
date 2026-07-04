# CLAUDE.md

基于 NautilusTrader + IBKR 的个人量化交易基础设施，实盘/回测双模，含前端可视化 Dashboard。

## 启动命令

```bash
# 引擎
python main.py                          # 实盘
python main.py --mode backtest          # 回测（上一交易日）
python main.py --mode backtest --date 2026-02-25

# 前端 WebSocket 服务器（在项目根目录执行）
node frontend/server.js                 # 端口 3000

# 测试下单
python order_sender.py                  # 市价单
python order_sender.py --bracket        # 括号单

# Redis 调试
redis-cli get account:funds
redis-cli hgetall position:NVDA
```

## 核心文件架构

```
main.py           # 入口：配置 TradingNode，注入 OrderGatewayActor，启动引擎
strategy.py       # 策略：1m/5m K线聚合、SuperTrend(10,2)/EMA21、日K围栏、写Redis
order_actor.py    # HTTP下单网关(端口8888) + 订单状态Redis回调 + 账户余额轮询
exit_manager.py   # 独立止盈/止损管理器，监听 bar.collected 事件
events.py         # 自定义 MessageBus 事件类（ExternalOrderCommand 等）
order_sender.py   # 外部下单测试脚本

frontend/
  server.js       # Node.js WebSocket + Redis订阅 + HTTP代理（端口3000）
  public/
    index.html        # 单图Dashboard：K线 + 右侧指标面板 + 语音提醒
    multi.html        # 四图总览：2×2网格，时间轴/十字线联动，止损拖动开仓
    indicators.html   # 四列指标排行：M1 ST / M5 ST / EMA偏离 / 日内新高
```

## 数据链路

| 链路 | 路径 |
|------|------|
| 实时K线 | IBKR → strategy.py → Redis PUBLISH → server.js → WebSocket → 浏览器 |
| 指标轮询 | 浏览器 → HTTP GET /api/\* → server.js → Redis GET（每30s） |
| 订单状态 | 浏览器下单 → order_actor.py(8888) → IBKR → Redis PUBLISH → WebSocket → 语音/Toast |
| 账户余额 | IBKR reqAccountSummary → AccountState 事件 → strategy.py → Redis → WebSocket |

Redis 是唯一共享状态，三层可独立重启。

## 设计约束

- **全部数据来自 IBKR**，无外部数据源依赖
- 实盘加载今日+昨日盘前数据（04:00 ET起），图表仅展示 RTH 09:30-16:00（390根K线）
- 盘后数据完全忽略
- NautilusTrader 内部使用 UTC 纳秒；图表展示使用 ET fake-UTC

## 前端代码注意事项

- 三个 HTML 文件各自独立，无共享 JS 模块
- 图表库：Lightweight Charts v4，通过 CDN 引入
- 语音提醒默认开启，右上角有切换按钮
- 订单标记使用 `orderMarkers` 全量数组管理，自动升序刷新（LightweightCharts API 要求）
- 止损拖动开仓/改止损价通过 drag 事件实现（mousedown → mousemove → mouseup）
- 价格线（入场价实线 + 止损价虚线）需在订单成交后实时绘制，页面刷新后从 `/api/active-orders` 恢复

## 常见问题处理

- `Connection refused` → 检查 TWS/Gateway 是否已启动且 API 已开启
- 无实时数据 → 将 `MARKET_DATA_TYPE` 改为 `IBMarketDataTypeEnum.DELAYED_FROZEN`
- 昨日围栏线不显示 → 确认 `history_days=2`，等引擎启动约60s后刷新
- 前端提示「引擎未连接」 → 重启前端：`node frontend/server.js`（项目根目录）
- FA Group 配置在 `main.py` 顶部：`FA_GROUP`、`FA_METHOD`、`ACCOUNT_ID`
