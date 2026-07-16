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

# 一键启动 + 音频监控（自启 Redis + 引擎 + 前端，8 步探测每步 macOS say 播报；双击 launch.command 同效）
bash scripts/launch.sh                  # 默认起全部；--no-engine 改为手动起 main.py（保留实时日志）

# 测试下单
python order_sender.py                  # 市价单
python order_sender.py --bracket        # 括号单

# Redis 调试
redis-cli get account:funds
redis-cli hgetall position:NVDA
```

## 核心文件架构

```
main.py           # 入口：配置 TradingNode，装配 M1/M5 指标策略 + OrderGatewayActor，启动引擎
indicators/       # 共享 IndicatorRegistry + 状态机（ST/EMA/DEMA/MomentumATR/SessionVWAP）
strategies/       # m1_indicator / m5_indicator 两周期策略：K线聚合 + 指标 → Redis + registry
signal_detector.py # M1 ST 翻转 + M5 同向 → st_super 信号 → 提案池（读 registry）
signals/          # st_super 信号；entry_methods（5 种进场方法 → TradeIntent）
entry/            # ticket_store：ARMED/RESTING 进场票据持久化（照搬 proposal_store 范式）
auto_runner.py    # Alpha + 控制台进场 → RiskGate → AutoPM 编排；消费 auto:enter / auto:close
execution/auto_pm.py # 仓位单元、OCA 止损/止盈、resting GTC 限价、对账、IBKR 报单
order_actor.py    # HTTP 网关(8888)：平仓/trail、/enter-now 立即触发；开仓 /order 已禁 403
exit_manager.py   # 独立止盈/止损管理器，监听 bar.collected 事件
events.py         # 自定义 MessageBus 事件类（EntryExecuteNowEvent 等）
order_sender.py   # 外部下单测试脚本

frontend/
  server.js       # Node.js WebSocket + Redis订阅 + HTTP代理（端口3000）
  public/
    console.html      # 控制台（交易主入口）：进场 / Agent执行 / 审批 / Kill Switch
    index.html        # 单图：纯图表查看（K线 + 指标 + 只读持仓/账户 + 语音）
    multi.html        # 四图：纯图表查看（2×2 时间轴/十字线联动）
    indicators.html   # 四列指标排行：M1 ST / M5 ST / EMA偏离 / 日内新高
```

## 数据链路

| 链路 | 路径 |
|------|------|
| 实时K线 | IBKR → strategies/m1·m5_indicator → Redis PUBLISH → server.js → WebSocket → 浏览器 |
| 指标 | strategies → IndicatorRegistry(内存) + indicators:active:{sym}(Redis)；信号/控制台读 |
| 指标轮询 | 浏览器 → HTTP GET /api/\* → server.js → Redis GET（每30s） |
| 进场/订单 | console → POST /api/enter → auto:enter → AutoRunner → AutoPM → IBKR → Redis PUBLISH → WebSocket |
| 账户余额 | IBKR reqAccountSummary → AccountState 事件 → 策略 → Redis → WebSocket |

Redis 是唯一共享状态，三层可独立重启。

## 设计约束

- **全部数据来自 IBKR**，无外部数据源依赖
- 实盘加载今日+昨日盘前数据（04:00 ET起），图表仅展示 RTH 09:30-16:00（390根K线）
- 盘后数据完全忽略
- NautilusTrader 内部使用 UTC 纳秒；图表展示使用 ET fake-UTC

## 前端代码注意事项

- 多个 HTML 页面各自独立；console.html 为交易主入口，index/multi 为纯图表查看页
- 图表库：Lightweight Charts v4，通过 CDN 引入；shared/*.js 提供 status-bar/toast/api-auth 等共享组件
- 语音提醒默认开启，状态栏铃铛切换
- 订单标记使用 `orderMarkers` 全量数组管理，自动升序刷新（LightweightCharts API 要求）
- 所有交易动作（开仓/平仓/改止损/审批）只在 console.html；index/multi 仅只读展示持仓价格线
- 价格线（入场价实线 + 止损价虚线）由 WS order:update + /api/active-orders 巡检驱动，页面刷新后恢复
- 进场统一经 `auto:enter:{sym}` → AutoRunner → AutoPM；旧 /api/order 已 403

## 常见问题处理

- `Connection refused` → 检查 TWS/Gateway 是否已启动且 API 已开启
- 无实时数据 → 将 `MARKET_DATA_TYPE` 改为 `IBMarketDataTypeEnum.DELAYED_FROZEN`
- 昨日围栏线不显示 → 确认 `history_days=2`，等引擎启动约60s后刷新
- 前端提示「引擎未连接」 → 重启前端：`node frontend/server.js`（项目根目录）
- FA Group 配置在 `main.py` 顶部：`FA_GROUP`、`FA_METHOD`、`ACCOUNT_ID`
